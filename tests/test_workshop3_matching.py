"""Exercise the matching SQL on an isolated PostgreSQL/PostGIS schema."""

import json

from src.matching import match_events
from test_workshop3_postgresql import PostgreSQLTestCase

EXACT = "2011-03-11T05:46:24Z"


class MatchingTests(PostgreSQLTestCase):
    def event(self, source, external_id, kind, timestamp=None, *, date=None, hour=None,
              minute=None, precision=None, lat=38.297, lon=142.373, group=None):
        return self.query("""
            INSERT INTO event
                (source, external_id, event_type, "timestamp", event_date, known_hour, known_minute,
                 time_precision, latitude, longitude, event_group_id, raw_reference)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
        """, (source, external_id, kind, timestamp, date, hour, minute, precision, lat, lon, group,
              json.dumps({"bronze_file": f"{source}.fixture", "external_id": external_id})))[0][0]

    def quake(self, external_id, timestamp=EXACT, magnitude=7.1, magnitude_type="mww",
              source="usgs_fdsn", **properties):
        event_id = self.event(source, external_id, "earthquake", timestamp, **properties)
        self.query("INSERT INTO earthquake (event_id, magnitude, magnitude_type) VALUES (%s, %s, %s)",
                   (event_id, magnitude, magnitude_type))
        return event_id

    def tsunami(self, external_id="noaa-one", timestamp=EXACT, precision="second",
                magnitude=7.0, cause=1, validity=4, **properties):
        event_id = self.event("noaa_hazel", external_id, "tsunami", timestamp, precision=precision, **properties)
        self.query("""
            INSERT INTO tsunami_event
                (event_id, cause_code, event_validity, reported_eq_magnitude, max_water_height)
            VALUES (%s, %s, %s, %s, 7.1)
        """, (event_id, cause, validity, magnitude))
        return event_id

    def relations(self, source, relation="caused_by"):
        return self.query("""
            SELECT to_event_id, review_status, evidence_reference, relation_id
            FROM event_relation WHERE from_event_id = %s AND relation_type = %s ORDER BY to_event_id
        """, (source, relation))

    def group(self, evidence):
        return self.query("""
            INSERT INTO event_group (event_type, evidence_reference)
            VALUES ('earthquake', %s) RETURNING event_group_id
        """, (evidence,))[0][0]

    def test_complete_timestamp_with_one_strong_candidate(self):
        tsunami = self.tsunami()
        quake = self.quake("usgs-close", timestamp="2011-03-11T05:46:04Z", lat=38.35)
        match_events(self.connection)

        target, status, reference, _ = self.relations(tsunami)[0]
        self.assertEqual((target, status), (quake, "accepted"))
        assessment = json.loads(reference)["assessment"]
        self.assertEqual(assessment["dt_s"], -20)
        self.assertEqual(assessment["time_precision"], "second")
        self.assertTrue(assessment["strong"])
        self.assertEqual(assessment["from_reference"]["external_id"], "noaa-one")

    def test_partial_precisions_respect_the_recorded_interval(self):
        # The UTC interval must not depend on the database session's daylight saving time.
        self.query("SET TIME ZONE 'America/New_York'")
        scenarios = [
            ("minute", "2026-11-01", 4, 58, "2026-11-01T04:58:59.9Z", "2026-11-01T04:59:00Z"),
            ("hour", "2026-11-02", 7, None, "2026-11-02T07:59:59Z", "2026-11-02T08:00:00Z"),
            ("day", "2026-11-01", None, None, "2026-11-01T23:59:59Z", "2026-11-02T00:00:00Z"),
        ]
        for index, (precision, date, hour, minute, inside, boundary) in enumerate(scenarios):
            with self.subTest(precision=precision):
                lat = 15 + index * 10
                source = self.tsunami(f"noaa-{precision}", None, precision, date=date, hour=hour, minute=minute, lat=lat)
                candidate = self.quake(f"{precision}-inside", timestamp=inside, lat=lat)
                self.quake(f"{precision}-outside", timestamp=boundary, lat=lat)
                match_events(self.connection)

                target, status, reference, _ = self.relations(source)[0]
                self.assertEqual((target, status), (candidate, "pending"))
                self.assertEqual(len(self.relations(source)), 1)
                assessment = json.loads(reference)["assessment"]
                self.assertIsNone(assessment["dt_s"])
                self.assertEqual(assessment["time_precision"], precision)
                self.assertEqual(self.query('SELECT "timestamp", known_hour, known_minute FROM event WHERE event_id=%s',
                                            (source,)), [(None, hour, minute)])

    def test_dated_and_nearby_is_insufficient_without_magnitude_and_seismic_cause(self):
        unknown_magnitude = self.tsunami("unknown-magnitude", None, "day", None, date="2011-03-11")
        wrong_magnitude = self.tsunami("wrong-magnitude", None, "day", 8.5, date="2011-03-11")
        landslide = self.tsunami("landslide", None, "day", 7.1, cause=8, date="2011-03-11")
        unknown_cause = self.tsunami("unknown-cause", None, "day", 7.1, cause=0, date="2011-03-11")
        self.quake("nearby")
        match_events(self.connection)
        for source in (unknown_magnitude, wrong_magnitude, landslide, unknown_cause):
            self.assertEqual(self.relations(source), [])

    def test_distinct_earthquakes_are_ambiguous_even_with_one_strong_match(self):
        tsunami = self.tsunami()
        self.quake("same-usgs-id")
        self.quake("same-usgs-id", source="usgs_realtime")
        match_events(self.connection)
        self.assertEqual([row[1] for row in self.relations(tsunami)], ["accepted", "accepted"])

        self.quake("different-usgs-id", timestamp="2011-03-11T05:54:24Z", lat=38.7)
        match_events(self.connection)
        links = self.relations(tsunami)
        self.assertEqual([row[1] for row in links], ["pending"] * 3)
        self.assertTrue(all(json.loads(row[2])["assessment"]["candidates"] == 2 for row in links))

    def test_changes_to_time_location_or_magnitude_invalidate_and_can_reactivate(self):
        source, target = self.tsunami(), self.quake("usgs-revised")
        match_events(self.connection)
        relation_id = self.relations(source)[0][3]
        for column, statement, changed, original in [
            ("time_ok", 'UPDATE event SET "timestamp" = %s WHERE event_id=%s', "2011-03-11T07:00:00Z", EXACT),
            ("distance_ok", "UPDATE event SET latitude = %s WHERE event_id=%s", 42, 38.297),
            ("magnitude_ok", "UPDATE earthquake SET magnitude = %s WHERE event_id=%s", 8.2, 7.1),
        ]:
            with self.subTest(criterion=column):
                self.query(statement, (changed, target))
                match_events(self.connection)
                link_target, status, ref, link = self.relations(source)[0]
                self.assertEqual((link_target, status, link), (target, "rejected", relation_id))
                evidence = json.loads(ref)
                self.assertEqual(evidence["state"], "invalidated")
                self.assertFalse(evidence["assessment"][column])
                self.assertEqual(evidence["last_match"]["generated_status"], "accepted")

                self.query(statement, (original, target))
                match_events(self.connection)
                self.assertEqual(self.relations(source)[0][1::2], ("accepted", relation_id))

    def test_incomplete_data_preserves_the_link_and_its_last_match(self):
        source, target = self.tsunami(), self.quake("usgs-incomplete")
        match_events(self.connection)
        reference, relation_id = self.relations(source)[0][2:]

        self.query("UPDATE event SET latitude = NULL WHERE event_id = %s", (target,))
        match_events(self.connection)
        link_target, status, ref, link = self.relations(source)[0]
        self.assertEqual((link_target, status, link), (target, "pending", relation_id))
        self.assertEqual(json.loads(ref)["state"], "insufficient_data")
        self.assertEqual(json.loads(ref)["last_match"], json.loads(reference))

        self.query("UPDATE event SET latitude = 38.297 WHERE event_id = %s", (target,))
        match_events(self.connection)
        self.assertEqual(self.relations(source)[0][1::2], ("accepted", relation_id))

    def test_manual_decision_survives_and_legacy_auto_link_is_reassessed(self):
        source = self.tsunami()
        target = self.quake("legacy-auto")
        manual_target = self.quake("manual")
        legacy = json.dumps({"rule": "tsunami_to_earthquake", "version": 1, "dt_s": 0,
                             "distance_km": 0, "candidates": 1})
        for quake, evidence in [(target, legacy), (manual_target, "Reviewed using a source bulletin")]:
            self.query("""
                INSERT INTO event_relation (from_event_id, to_event_id, relation_type, review_status, evidence_reference)
                VALUES (%s, %s, 'caused_by', 'accepted', %s)
            """, (source, quake, evidence))
        self.query("UPDATE event SET latitude = 42 WHERE event_type = 'earthquake'")
        match_events(self.connection)
        _, status, reference, _ = self.relations(source)[0]
        self.assertEqual(status, "rejected")
        self.assertEqual(json.loads(reference)["last_match"], json.loads(legacy))
        self.assertEqual(self.relations(source)[1][1:3], ("accepted", "Reviewed using a source bulletin"))

    def test_manual_status_change_alone_is_preserved(self):
        source = self.tsunami()
        self.quake("candidate", timestamp="2011-03-11T05:54:24Z")
        match_events(self.connection)
        _, status, reference, relation_id = self.relations(source)[0]
        self.assertEqual(status, "pending")
        self.query("UPDATE event_relation SET review_status = 'accepted' WHERE relation_id = %s", (relation_id,))
        match_events(self.connection)
        self.assertEqual(self.relations(source)[0][1:3], ("accepted", reference))

    def test_cap_uses_origin_and_requires_comparable_magnitude_for_automatic_acceptance(self):
        source = self.event("ntwc", "cap-one", "bulletin", "2011-03-11T05:56:24Z")
        self.query("""
            INSERT INTO tsunami_bulletin
                (event_id, center, cap_identifier, reported_origin_at, preliminary_magnitude, preliminary_magnitude_type)
            VALUES (%s, 'ntwc', 'cap-one', %s, 7.1, 'Mwp')
        """, (source, EXACT))
        self.quake("same-origin")
        match_events(self.connection)
        self.assertEqual(self.relations(source, "reports_on")[0][1], "pending")
        self.query("UPDATE tsunami_bulletin SET preliminary_magnitude_type = ' Mww '")
        match_events(self.connection)
        self.assertEqual(self.relations(source, "reports_on")[0][1], "accepted")

    def test_merge_moves_all_members_into_one_group_and_replay_is_stable(self):
        kept, merged = self.group("catalog review"), self.group("real-time review")
        for label in ("shared-id-1", "shared-id-2"):
            self.quake(label, source="usgs_fdsn", group=kept)
            self.quake(label, source="usgs_realtime", group=merged)
        alias = self.quake("alias-in-merged-group", source="usgs_realtime", group=merged)

        match_events(self.connection)
        self.assertEqual(self.query("SELECT event_group_id, count(*) FROM event GROUP BY event_group_id"), [(kept, 5)])
        self.assertEqual(self.query("SELECT event_id FROM event WHERE event_id = %s", (alias,)), [(alias,)])
        surviving = self.query("SELECT event_group_id, evidence_reference FROM event_group")
        self.assertEqual(len(surviving), 1)
        self.assertEqual(json.loads(surviving[0][1])["prior_groups"],
                         [{"id": kept, "evidence": "catalog review"}, {"id": merged, "evidence": "real-time review"}])
        query = "SELECT relation_id, review_status, evidence_reference, xmin::text FROM event_relation ORDER BY relation_id"
        before = self.query(query)
        self.assertEqual(len(before), 2)
        match_events(self.connection)
        self.assertEqual(self.query(query), before)
        self.assertEqual(self.query("SELECT event_group_id, evidence_reference FROM event_group"), surviving)

    def test_group_conflicts_from_manual_rejection_are_left_for_review(self):
        group_a, group_b = self.group("review A"), self.group("review B")
        fdsn = self.quake("shared", group=group_a)
        realtime = self.quake("shared", source="usgs_realtime", group=group_b)
        a = self.quake("alias-a", group=group_a)
        b = self.quake("alias-b", source="usgs_realtime", group=group_b)
        self.query("""
            INSERT INTO event_relation (from_event_id, to_event_id, relation_type, review_status, evidence_reference)
            VALUES (%s, %s, 'identity_candidate', 'rejected', 'conflicting manual review')
        """, (b, a))
        match_events(self.connection)
        target, status, reference, _ = self.relations(realtime, "identity_candidate")[0]
        self.assertEqual((target, status), (fdsn, "pending"))
        self.assertTrue(json.loads(reference)["assessment"]["group_conflict"])
        self.assertEqual(self.query("SELECT count(*) FROM event_group"), [(2,)])
