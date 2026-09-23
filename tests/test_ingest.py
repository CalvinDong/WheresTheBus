import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
import zipfile

from google.transit import gtfs_realtime_pb2
from ingest import collect, inspect_payload, make_session


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def session(self, payload, status=200):
        response = Mock(status_code=status, headers={})
        response.iter_content.return_value = [payload]
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        return Mock(get=Mock(return_value=response))

    def test_realtime_preserves_zero_absence_and_cancellation(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        entity = feed.entity.add(id="trip")
        entity.trip_update.trip.trip_id = "t1"
        entity.trip_update.stop_time_update.add(stop_id="a").arrival.delay = 0
        entity.trip_update.stop_time_update.add(stop_id="b")
        cancelled = feed.entity.add(id="cancelled")
        cancelled.trip_update.trip.trip_id = "t2"
        cancelled.trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
        feed.entity.add(id="deleted", is_deleted=True)
        raw = feed.SerializeToString()
        folder = collect(self.session(raw), "realtime", self.root)
        self.assertEqual((folder / "feed.pb").read_bytes(), raw)
        metadata = json.loads((folder / "metadata.json").read_text())
        self.assertEqual(metadata["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertIsNone(metadata["feed_timestamp"])
        entities = json.loads((folder / "feed.json").read_text())["entity"]
        stops = entities[0]["trip_update"]["stop_time_update"]
        self.assertEqual(stops[0]["arrival"]["delay"], 0)
        self.assertNotIn("arrival", stops[1])
        self.assertEqual(entities[1]["trip_update"]["trip"]["schedule_relationship"], "CANCELED")
        self.assertTrue(entities[2]["is_deleted"])

    def test_invalid_payload_never_publishes_manifest(self):
        with self.assertRaises(Exception):
            collect(self.session(b"<html>error</html>"), "realtime", self.root)
        self.assertFalse(list(self.root.rglob("metadata.json")))
        self.assertFalse(list(self.root.rglob("download.partial")))

    def test_unauthorized_never_publishes_manifest(self):
        with self.assertRaisesRegex(RuntimeError, "401"):
            collect(self.session(b"denied", status=401), "static", self.root)
        self.assertFalse(list(self.root.rglob("metadata.json")))

    def test_static_nested_bundle(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("operator.zip", b"nested archive validated downstream")
        folder = collect(self.session(buffer.getvalue()), "static", self.root)
        details, decoded = inspect_payload(folder / "feed.zip", "static")
        self.assertEqual(details["archive_members"], ["operator.zip"])
        self.assertIsNone(decoded)

    def test_retries_are_bounded_and_exclude_auth_errors(self):
        with make_session("test-key") as session:
            retry = session.get_adapter("https://").max_retries
            self.assertEqual(retry.total, 4)
            self.assertIn(429, retry.status_forcelist)
            self.assertNotIn(401, retry.status_forcelist)


if __name__ == "__main__":
    unittest.main()
