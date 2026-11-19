"""Event identification across sources (Workshop 3).

Links observations of the same phenomenon in event_relation, using the thresholds of the
event_matching block of config/sources.yaml:

- caused_by: HazEL tsunami (earthquake cause) -> USGS earthquake, by origin time and distance.
  The HazEL earthquake id is NOAA's own catalog id, not a USGS id, so it cannot be used to join.
- reports_on: NTWC/PTWC bulletin -> USGS earthquake, by the CAP origin time, distance and magnitude.
- identity_candidate: USGS real-time record -> USGS FDSN record with the same USGS id; both are
  then put in the same event_group.

Every rule runs over the whole event table, so the result does not depend on the order of the
loads, and the inserts are idempotent (UNIQUE (from_event_id, to_event_id, relation_type)).
A link with exactly one candidate is 'accepted'; several candidates stay 'pending' for review.
Automatic links keep their evidence as JSON starting with {"rule"; a link someone reviewed by
hand (other evidence, or 'rejected') is never overwritten.
"""

import json
import logging

from src.config import load_matching_rules

log = logging.getLogger(__name__)

UPSERT = """
INSERT INTO event_relation (from_event_id, to_event_id, relation_type, review_status, evidence_reference)
SELECT from_id, to_id, %(relation)s, CASE WHEN candidates = 1 THEN 'accepted' ELSE 'pending' END,
       json_build_object('rule', %(rule)s, 'version', %(version)s, 'dt_s', round(dt_s::numeric, 1),
                         'distance_km', round(distance_km::numeric, 1), 'candidates', candidates)::text
FROM ranked
ON CONFLICT (from_event_id, to_event_id, relation_type) DO UPDATE
    SET review_status = EXCLUDED.review_status, evidence_reference = EXCLUDED.evidence_reference
    WHERE event_relation.review_status <> 'rejected'
      AND event_relation.evidence_reference LIKE '{"rule"%%'
      AND event_relation.evidence_reference IS DISTINCT FROM EXCLUDED.evidence_reference
"""

# Candidates are counted by distinct USGS id: the same earthquake from the real-time feed and
# from FDSN is one candidate, not two.
RANKED = """
, ranked AS (
    SELECT pairs.*, counts.candidates
    FROM pairs
    JOIN (SELECT from_id, count(DISTINCT to_external_id) AS candidates FROM pairs GROUP BY from_id) counts
      USING (from_id)
)
"""

TSUNAMI_PAIRS = """
WITH pairs AS (
    SELECT t.event_id AS from_id, e.event_id AS to_id, e.external_id AS to_external_id,
           abs(extract(epoch FROM e."timestamp" - t."timestamp")) AS dt_s,
           ST_Distance(t.geom::geography, e.geom::geography) / 1000 AS distance_km
    FROM event t
    JOIN tsunami_event te ON te.event_id = t.event_id
    JOIN event e ON e.event_type = 'earthquake'
    WHERE t.event_type = 'tsunami'
      AND te.cause_code = ANY(%(cause_codes)s)
      AND e."timestamp" BETWEEN t."timestamp" - make_interval(secs => %(seconds)s)
                            AND t."timestamp" + make_interval(secs => %(seconds)s)
      AND ST_DWithin(t.geom::geography, e.geom::geography, %(meters)s)
)
"""

BULLETIN_PAIRS = """
WITH pairs AS (
    SELECT b.event_id AS from_id, e.event_id AS to_id, e.external_id AS to_external_id,
           abs(extract(epoch FROM e."timestamp" - tb.reported_origin_at)) AS dt_s,
           ST_Distance(b.geom::geography, e.geom::geography) / 1000 AS distance_km
    FROM event b
    JOIN tsunami_bulletin tb ON tb.event_id = b.event_id
    JOIN event e ON e.event_type = 'earthquake'
    JOIN earthquake q ON q.event_id = e.event_id
    WHERE b.event_type = 'bulletin'
      AND e."timestamp" BETWEEN tb.reported_origin_at - make_interval(secs => %(seconds)s)
                            AND tb.reported_origin_at + make_interval(secs => %(seconds)s)
      AND ST_DWithin(b.geom::geography, e.geom::geography, %(meters)s)
      AND (tb.preliminary_magnitude IS NULL OR q.magnitude IS NULL
           OR abs(tb.preliminary_magnitude - q.magnitude) <= %(max_magnitude_difference)s)
)
"""

IDENTITY_PAIRS = """
WITH pairs AS (
    SELECT r.event_id AS from_id, f.event_id AS to_id, f.external_id AS to_external_id,
           abs(extract(epoch FROM f."timestamp" - r."timestamp")) AS dt_s,
           ST_Distance(r.geom::geography, f.geom::geography) / 1000 AS distance_km
    FROM event r
    JOIN event f ON f.external_id = r.external_id AND f.source = 'usgs_fdsn'
    WHERE r.source = 'usgs_realtime' AND r.event_type = 'earthquake' AND f.event_type = 'earthquake'
)
"""

# Accepted identity links: put both records in one event_group (reusing a group either one has)
GROUP_PAIRS = """
SELECT rel.from_event_id, rel.to_event_id, a.event_group_id, b.event_group_id
FROM event_relation rel
JOIN event a ON a.event_id = rel.from_event_id
JOIN event b ON b.event_id = rel.to_event_id
WHERE rel.relation_type = 'identity_candidate' AND rel.review_status = 'accepted'
  AND (a.event_group_id IS NULL OR b.event_group_id IS NULL OR a.event_group_id <> b.event_group_id)
"""

SUMMARY = """
SELECT relation_type, review_status, count(*) FROM event_relation
GROUP BY relation_type, review_status ORDER BY relation_type, review_status
"""


def _group_identities(cursor):
    cursor.execute(GROUP_PAIRS)
    grouped = 0
    for from_id, to_id, from_group, to_group in cursor.fetchall():
        group = to_group or from_group
        if group is None:
            cursor.execute(
                "INSERT INTO event_group (event_type, evidence_reference) "
                "VALUES ('earthquake', 'same USGS id (identity_candidate)') RETURNING event_group_id")
            group = cursor.fetchone()[0]
        cursor.execute("UPDATE event SET event_group_id = %s WHERE event_id IN (%s, %s)", (group, from_id, to_id))
        grouped += 1
    return grouped


def match_events(connection, rules=None):
    """Run every matching rule in one transaction; returns {relation_type: {status: count}}."""
    rules = rules or load_matching_rules()
    version = rules["rule_version"]
    tsunami, bulletin = rules["tsunami_to_earthquake"], rules["bulletin_to_earthquake"]
    steps = [
        ("caused_by", "tsunami_to_earthquake", TSUNAMI_PAIRS,
         {"cause_codes": tsunami["cause_codes"], "seconds": tsunami["max_time_seconds"],
          "meters": tsunami["max_distance_km"] * 1000}),
        ("reports_on", "bulletin_to_earthquake", BULLETIN_PAIRS,
         {"seconds": bulletin["max_time_seconds"], "meters": bulletin["max_distance_km"] * 1000,
          "max_magnitude_difference": bulletin["max_magnitude_difference"]}),
    ]
    if rules.get("same_usgs_id", True):
        steps.append(("identity_candidate", "same_usgs_id", IDENTITY_PAIRS, {}))

    with connection:  # commit on success, roll back on error
        with connection.cursor() as cursor:
            for relation, rule, pairs, params in steps:
                cursor.execute(pairs + RANKED + UPSERT, {**params, "relation": relation, "rule": rule,
                                                         "version": version})
                log.info("%s: %d link(s) inserted or updated", relation, cursor.rowcount)
            grouped = _group_identities(cursor)
            cursor.execute(SUMMARY)
            summary = {}
            for relation, status, count in cursor.fetchall():
                summary.setdefault(relation, {})[status] = count
    log.info("event_relation: %s; %d identity pair(s) grouped", json.dumps(summary), grouped)
    return summary
