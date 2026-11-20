"""Reconcile candidate links and verified USGS identities in the canonical database."""

import json
import logging
from collections import defaultdict

from src.config import load_matching_rules

log = logging.getLogger(__name__)
MATCHER = "workshop3_matching"

TSUNAMI_ORIGINS = """
WITH origins AS (
    SELECT t.event_id, t.geom, t.raw_reference, t.time_precision,
           CASE WHEN t.time_precision IN ('second', 'subsecond') THEN t."timestamp" END AS origin_time,
           -- These clock values bound an interval. They are not timestamps assigned to observations.
           (t.event_date + CASE t.time_precision
               WHEN 'minute' THEN make_time(t.known_hour, t.known_minute, 0)
               WHEN 'hour' THEN make_time(t.known_hour, 0, 0)
               WHEN 'day' THEN time '00:00'
           END) AT TIME ZONE 'UTC' AS bucket_start,
           CASE t.time_precision
               WHEN 'minute' THEN interval '1 minute'
               WHEN 'hour' THEN interval '1 hour'
               WHEN 'day' THEN interval '24 hours'
           END AS bucket_width,
           NULLIF(te.cause_code, 0) = ANY(%(cause_codes)s)
               AND COALESCE(te.event_validity > 0, true) AS source_ok,
           te.cause_code = 1 AND te.event_validity = ANY(%(auto_validity_codes)s) AS strong_source,
           te.reported_eq_magnitude AS magnitude, NULL::text AS magnitude_type,
           t.time_precision IN ('hour', 'day') AS magnitude_required
    FROM event t LEFT JOIN tsunami_event te ON te.event_id = t.event_id
    WHERE t.source = 'noaa_hazel' AND t.event_type = 'tsunami'
)
"""

BULLETIN_ORIGINS = """
WITH origins AS (
    SELECT b.event_id, b.geom, b.raw_reference, 'reported_origin' AS time_precision,
           tb.reported_origin_at AS origin_time, NULL::timestamptz AS bucket_start,
           NULL::interval AS bucket_width, true AS source_ok, true AS strong_source,
           tb.preliminary_magnitude AS magnitude, tb.preliminary_magnitude_type AS magnitude_type,
           false AS magnitude_required
    FROM event b LEFT JOIN tsunami_bulletin tb ON tb.event_id = b.event_id
    WHERE b.source IN ('ntwc', 'ptwc') AND b.event_type = 'bulletin'
)
"""

PROXIMITY_PAIRS = """
, time_windows AS (
    SELECT *,
           COALESCE(origin_time - make_interval(secs => %(seconds)s), bucket_start) AS window_start,
           COALESCE(origin_time + make_interval(secs => %(seconds)s), bucket_start + bucket_width) AS window_end
    FROM origins
), pair_ids AS (
    SELECT w.event_id AS from_id, e.event_id AS to_id
    FROM time_windows w
    JOIN event e ON e."timestamp" BETWEEN w.window_start AND w.window_end
                AND e.event_type = 'earthquake' AND e.source IN ('usgs_realtime', 'usgs_fdsn')
    WHERE w.source_ok
      AND ST_DWithin(w.geom::geography, e.geom::geography, %(distance_km)s * 1000)
    UNION
    -- Reassess existing links even if their endpoints are now outside the search window.
    SELECT from_event_id, to_event_id FROM event_relation WHERE relation_type = %(relation)s
), measures AS (
    SELECT p.*, e.external_id, w.time_precision, w.window_start, w.window_end,
           w.event_id IS NOT NULL AND e.event_type = 'earthquake'
               AND e.source IN ('usgs_realtime', 'usgs_fdsn') AS endpoints_ok,
           w.source_ok, w.strong_source, w.magnitude_required,
           w.raw_reference AS from_reference, e.raw_reference AS to_reference,
           CASE WHEN q.event_id IS NOT NULL THEN true END AS has_earthquake,
           e."timestamp" >= w.window_start AND
               (e."timestamp" < w.window_end OR (w.origin_time IS NOT NULL AND e."timestamp" = w.window_end))
               AS time_ok,
           extract(epoch FROM e."timestamp" - w.origin_time) AS dt_s,
           ST_Distance(w.geom::geography, e.geom::geography) / 1000 AS distance_km,
           abs(w.magnitude - q.magnitude) AS magnitude_difference,
           lower(NULLIF(btrim(w.magnitude_type), '')) = lower(NULLIF(btrim(q.magnitude_type), ''))
               AS same_magnitude_type
    FROM pair_ids p
    LEFT JOIN time_windows w ON w.event_id = p.from_id
    JOIN event e ON e.event_id = p.to_id
    LEFT JOIN earthquake q ON q.event_id = e.event_id
), checks AS (
    SELECT *,
           distance_km <= %(distance_km)s AS distance_ok,
           CASE WHEN magnitude_difference IS NOT NULL THEN magnitude_difference <= %(magnitude_difference)s
                WHEN NOT magnitude_required THEN true
           END AS magnitude_ok
    FROM measures
)
SELECT from_id, to_id, external_id,
       endpoints_ok AND source_ok AND has_earthquake AND time_ok AND distance_ok AND magnitude_ok AS eligible,
       strong_source AND abs(dt_s) <= %(auto_seconds)s AND distance_km <= %(auto_distance_km)s
           AND magnitude_difference <= %(magnitude_difference)s
           AND (NOT %(require_same_magnitude_type)s OR same_magnitude_type) AS strong,
       jsonb_build_object(
           'time_precision', time_precision, 'window_start_utc', window_start AT TIME ZONE 'UTC',
           'window_end_utc', window_end AT TIME ZONE 'UTC', 'dt_s', round(dt_s, 3),
           'distance_km', round(distance_km::numeric, 3), 'magnitude_difference', magnitude_difference,
           'same_magnitude_type', same_magnitude_type, 'source_ok', source_ok,
           'time_ok', time_ok, 'distance_ok', distance_ok, 'magnitude_ok', magnitude_ok,
           'from_reference', from_reference, 'to_reference', to_reference
       ) AS assessment
FROM checks
"""

IDENTITY_PAIRS = """
WITH pair_ids AS (
    SELECT r.event_id AS from_id, f.event_id AS to_id
    FROM event r
    JOIN event f ON f.external_id = r.external_id AND f.source = 'usgs_fdsn'
    WHERE r.source = 'usgs_realtime' AND r.event_type = 'earthquake' AND f.event_type = 'earthquake'
    UNION
    SELECT from_event_id, to_event_id FROM event_relation WHERE relation_type = %(relation)s
)
SELECT p.from_id, p.to_id, f.external_id,
       r.source = 'usgs_realtime' AND f.source = 'usgs_fdsn'
           AND r.event_type = 'earthquake' AND f.event_type = 'earthquake'
           AND r.external_id = f.external_id AS eligible,
       true AS strong,
       jsonb_build_object('from_external_id', r.external_id, 'to_external_id', f.external_id,
                          'from_reference', r.raw_reference, 'to_reference', f.raw_reference)
FROM pair_ids p
JOIN event r ON r.event_id = p.from_id
JOIN event f ON f.event_id = p.to_id
"""

SUMMARY = """
SELECT relation_type, review_status, count(*) FROM event_relation
GROUP BY relation_type, review_status ORDER BY relation_type, review_status
"""

LEGACY_KEYS = {"rule", "version", "dt_s", "distance_km", "candidates"}
MANAGED_KEYS = {"managed_by", "rule", "version", "generated_status", "state", "assessment", "last_match"}


def _automatic_evidence(status, reference, rule):
    """Recognize our own decisions, including records generated by the original matcher."""
    try:
        evidence = json.loads(reference) if reference else None
    except (TypeError, ValueError):
        return None
    if not isinstance(evidence, dict) or evidence.get("rule") != rule:
        return None
    if evidence.get("managed_by") == MATCHER and evidence.keys() <= MANAGED_KEYS:
        return evidence if evidence.get("generated_status") == status else None
    if (evidence.keys() == LEGACY_KEYS and evidence["version"] == 1
            and type(evidence["candidates"]) is int and evidence["candidates"] > 0):
        generated_status = "accepted" if evidence["candidates"] == 1 else "pending"
        return evidence if generated_status == status else None
    return None


def _make_evidence(rule, version, status, state, assessment, previous):
    evidence = {
        "managed_by": MATCHER, "rule": rule, "version": version,
        "generated_status": status, "state": state, "assessment": assessment,
    }
    if previous:
        last_match = previous.get("last_match")
        was_eligible = previous.get("state", "eligible") == "eligible"
        lost_acceptance = previous.get("generated_status") == "accepted" and status != "accepted"
        if was_eligible and (state != "eligible" or lost_acceptance):
            last_match = {key: value for key, value in previous.items() if key != "last_match"}
        if last_match is not None:
            evidence["last_match"] = last_match
    return json.dumps(evidence, sort_keys=True)


def _reconcile(cursor, relation, rule, version, pairs):
    cursor.execute("""
        SELECT from_event_id, to_event_id, relation_id, review_status, evidence_reference
        FROM event_relation WHERE relation_type = %s
    """, (relation,))
    existing = {(source, target): (link, status, evidence)
                for source, target, link, status, evidence in cursor.fetchall()}
    # A real-time and a catalog observation with the same USGS ID count as one candidate.
    candidates = defaultdict(set)
    for source, _, external_id, eligible, _, _ in pairs:
        if eligible:
            candidates[source].add(external_id)

    changed = 0
    for source, target, _, eligible, strong, assessment in pairs:
        old = existing.get((source, target))
        previous = _automatic_evidence(old[1], old[2], rule) if old else None
        if old and previous is None:
            continue
        count = len(candidates[source])
        if eligible:
            if relation == "identity_candidate":
                conflict = _group_conflict(cursor, source, target)
                strong = not conflict
                assessment["group_conflict"] = conflict
            state = "eligible"
            status = "accepted" if count == 1 and strong else "pending"
        elif old:
            # A NULL result means missing inputs, not evidence against the match.
            if eligible is None and previous.get("state") == "invalidated":
                continue
            state = "insufficient_data" if eligible is None else "invalidated"
            status = "pending" if eligible is None else "rejected"
        else:
            continue
        assessment = {**assessment, "candidates": count, "strong": strong is True}
        evidence = _make_evidence(rule, version, status, state, assessment, previous)
        if old:
            link, old_status, old_evidence = old
            if (status, evidence) == (old_status, old_evidence):
                continue
            # Keep a manual review made between reading and writing the row.
            cursor.execute("""
                UPDATE event_relation SET review_status = %s, evidence_reference = %s
                WHERE relation_id = %s AND review_status = %s AND evidence_reference IS NOT DISTINCT FROM %s
            """, (status, evidence, link, old_status, old_evidence))
        else:
            cursor.execute("""
                INSERT INTO event_relation (from_event_id, to_event_id, relation_type, review_status, evidence_reference)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (from_event_id, to_event_id, relation_type) DO NOTHING
            """, (source, target, relation, status, evidence))
        changed += cursor.rowcount
    return changed


GROUP_PAIRS = """
SELECT rel.relation_id, rel.from_event_id, rel.to_event_id, rel.evidence_reference
FROM event_relation rel
JOIN event a ON a.event_id = rel.from_event_id
JOIN event b ON b.event_id = rel.to_event_id
WHERE rel.relation_type = 'identity_candidate' AND rel.review_status = 'accepted'
  AND a.event_type = 'earthquake' AND b.event_type = 'earthquake'
  AND (a.event_group_id IS NULL OR b.event_group_id IS NULL OR a.event_group_id <> b.event_group_id)
ORDER BY rel.relation_id
FOR NO KEY UPDATE OF rel
"""


def _group_conflict(cursor, from_id, to_id):
    cursor.execute("""
        WITH group_ids AS (
            SELECT event_group_id FROM event WHERE event_id IN (%s, %s)
        ), members AS (
            SELECT event_id, event_type FROM event
            WHERE event_group_id IN (SELECT event_group_id FROM group_ids) OR event_id IN (%s, %s)
        )
        SELECT EXISTS (
            SELECT 1 FROM event_group
            WHERE event_group_id IN (SELECT event_group_id FROM group_ids) AND event_type <> 'earthquake'
        ) OR EXISTS (SELECT 1 FROM members WHERE event_type <> 'earthquake')
          OR EXISTS (
              SELECT 1 FROM event_relation rel
              JOIN members a ON a.event_id = rel.from_event_id
              JOIN members b ON b.event_id = rel.to_event_id
              WHERE rel.relation_type = 'identity_candidate' AND rel.review_status = 'rejected'
          )
    """, (from_id, to_id, from_id, to_id))
    return cursor.fetchone()[0]


def _group_identities(cursor):
    cursor.execute(GROUP_PAIRS)
    grouped = 0
    for relation_id, from_id, to_id, reference in cursor.fetchall():
        # A preceding merge in this same run may have changed either endpoint.
        cursor.execute("SELECT event_group_id FROM event WHERE event_id IN (%s, %s) ORDER BY event_id FOR UPDATE",
                       (from_id, to_id))
        groups = [group for (group,) in cursor.fetchall()]
        group_ids = sorted({group for group in groups if group is not None})
        if len(group_ids) == 1 and None not in groups:
            continue
        cursor.execute("""
            SELECT event_group_id, evidence_reference FROM event_group
            WHERE event_group_id = ANY(%s) ORDER BY event_group_id FOR UPDATE
        """, (group_ids,))
        current = cursor.fetchall()
        if _group_conflict(cursor, from_id, to_id):
            # A preceding merge may have introduced a conflict. Keep any reviewed decision.
            previous = _automatic_evidence("accepted", reference, "same_usgs_id")
            if previous and previous.get("managed_by") == MATCHER:
                assessment = {**previous["assessment"], "group_conflict": True, "strong": False}
                evidence = _make_evidence("same_usgs_id", previous["version"], "pending", "eligible",
                                          assessment, previous)
                cursor.execute("""
                    UPDATE event_relation SET review_status = 'pending', evidence_reference = %s
                    WHERE relation_id = %s
                """, (evidence, relation_id))
            log.warning("Identity relation %s conflicts with current group membership or a rejection", relation_id)
            continue
        if not current:
            cursor.execute("""
                INSERT INTO event_group (event_type, evidence_reference)
                VALUES ('earthquake', %s) RETURNING event_group_id
            """, (json.dumps({"identity_relation_id": relation_id}),))
            group = cursor.fetchone()[0]
        else:
            group = group_ids[0]
            if len(current) > 1:
                evidence = {
                    "identity_relation_id": relation_id,
                    "prior_groups": [{"id": group_id, "evidence": note} for group_id, note in current],
                }
                cursor.execute("UPDATE event_group SET evidence_reference = %s WHERE event_group_id = %s",
                               (json.dumps(evidence), group))

        cursor.execute("""
            UPDATE event SET event_group_id = %s
            WHERE (event_id IN (%s, %s) OR event_group_id = ANY(%s))
              AND event_group_id IS DISTINCT FROM %s
        """, (group, from_id, to_id, group_ids, group))
        for obsolete in group_ids[1:]:
            cursor.execute("DELETE FROM event_group WHERE event_group_id = %s", (obsolete,))
        grouped += 1
    return grouped


def _proximity_parameters(rule, require_same_magnitude_type):
    return {
        "seconds": rule["max_time_seconds"], "distance_km": rule["max_distance_km"],
        "magnitude_difference": rule["max_magnitude_difference"],
        "auto_seconds": rule.get("auto_accept_time_seconds", rule["max_time_seconds"]),
        "auto_distance_km": rule["auto_accept_distance_km"],
        "require_same_magnitude_type": require_same_magnitude_type,
    }


def match_events(connection, rules=None):
    """Reassess links and group verified identities in one transaction."""
    rules = rules or load_matching_rules()
    tsunami, bulletin = rules["tsunami_to_earthquake"], rules["bulletin_to_earthquake"]
    steps = [
        ("caused_by", "tsunami_to_earthquake", TSUNAMI_ORIGINS + PROXIMITY_PAIRS,
         {**_proximity_parameters(tsunami, False), "cause_codes": tsunami["cause_codes"],
          "auto_validity_codes": tsunami["auto_validity_codes"]}),
        ("reports_on", "bulletin_to_earthquake", BULLETIN_ORIGINS + PROXIMITY_PAIRS,
         _proximity_parameters(bulletin, True)),
    ]
    if rules.get("same_usgs_id", True):
        steps.append(("identity_candidate", "same_usgs_id", IDENTITY_PAIRS, {}))

    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('tsunami-monitoring.match_events'))")
        for relation, rule, query, params in steps:
            cursor.execute(query, {**params, "relation": relation})
            changed = _reconcile(cursor, relation, rule, rules["rule_version"], cursor.fetchall())
            log.info("%s: %d link(s) inserted or reassessed", relation, changed)
        grouped = _group_identities(cursor)
        cursor.execute(SUMMARY)
        summary = {}
        for relation, status, count in cursor.fetchall():
            summary.setdefault(relation, {})[status] = count
    log.info("event_relation: %s; %d identity pair(s) grouped", json.dumps(summary), grouped)
    return summary
