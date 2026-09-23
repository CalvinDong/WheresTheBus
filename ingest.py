"""Collect TfNSW bus feeds into a local, replayable Bronze layer."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import time
from uuid import uuid4
import zipfile

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

BASE = "https://api.transport.nsw.gov.au/v1/gtfs"
URLS = {"static": f"{BASE}/schedule/buses", "realtime": f"{BASE}/realtime/buses"}
LOG = logging.getLogger(__name__)


def make_session(api_key):
    session = requests.Session()
    session.headers.update({"Authorization": f"apikey {api_key}"})
    retries = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"], respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def inspect_payload(path, kind):
    """Validate the container; preserve all protobuf entities and optional fields."""
    if kind == "static":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            # The all-bus bundle can contain operator ZIPs instead of CSVs.
            if not any(name.lower().endswith((".txt", ".zip")) for name in names):
                raise ValueError("Static ZIP contains no GTFS tables or operator archives")
        return {"archive_members": names}, None
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())
    if not feed.IsInitialized():
        raise ValueError("Realtime feed is missing required protobuf fields")
    details = {"entity_count": len(feed.entity),
               "feed_timestamp": feed.header.timestamp if feed.header.HasField("timestamp") else None}
    return details, MessageToDict(feed, preserving_proto_field_name=True)


def collect(session, kind, root):
    """Publish metadata last: its presence marks a completed snapshot."""
    started = datetime.now(timezone.utc)
    snapshot_id = started.strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex
    folder = root / "bronze" / f"gtfs_{kind}" / started.strftime("date=%Y-%m-%d/hour=%H") / snapshot_id
    folder.mkdir(parents=True)
    payload = folder / ("feed.zip" if kind == "static" else "feed.pb")
    temporary = folder / "download.partial"
    digest = hashlib.sha256()
    size = 0
    try:
        with session.get(URLS[kind], stream=True, timeout=(10, 180), allow_redirects=False) as response:
            if response.status_code != 200:
                # Do not log response bodies or request headers containing credentials.
                raise RuntimeError(f"TfNSW returned HTTP {response.status_code}; check API access if 401/403")
            with temporary.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    output.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            headers = {key: response.headers.get(key) for key in ("Content-Type", "ETag", "Last-Modified")}
        if size == 0:
            raise ValueError("TfNSW returned an empty payload")
        temporary.replace(payload)
        details, decoded = inspect_payload(payload, kind)
        if decoded is not None:
            write_json(folder / "feed.json", decoded)
        write_json(folder / "metadata.json", {
            "schema_version": 1, "snapshot_id": snapshot_id, "source": "tfnsw",
            "kind": kind, "source_url": URLS[kind], "requested_at_utc": started.isoformat(),
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "payload_file": payload.name, "sha256": digest.hexdigest(),
            "size_bytes": size, "response_headers": headers, **details,
        })
    finally:
        temporary.unlink(missing_ok=True)
    LOG.info("Saved %s snapshot (%d bytes): %s", kind, size, folder)
    return folder


def write_json(path, value):
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def demo(root):
    """Exercise the exact ingestion path with synthetic data and no credentials."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())
    entity = feed.entity.add(id="demo-entity")
    entity.trip_update.trip.trip_id = "demo-trip"
    entity.trip_update.trip.route_id = "demo-route"
    stop = entity.trip_update.stop_time_update.add(stop_id="demo-stop", stop_sequence=1)
    stop.arrival.delay = 0

    class Response:
        status_code = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def iter_content(self, chunk_size):
            yield feed.SerializeToString()

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    # Separate synthetic records from real observations.
    return collect(Session(), "realtime", root / "demo")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["static", "realtime", "demo"])
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--interval", type=int, help="Poll realtime every N seconds (minimum 30)")
    args = parser.parse_args()
    if args.interval is not None and (args.kind != "realtime" or args.interval < 30):
        parser.error("--interval requires realtime and must be at least 30 seconds")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.kind == "demo":
        demo(args.output)
        return
    key = os.environ.get("TFNSW_API_KEY", "").strip()
    if not key:
        parser.error("Set TFNSW_API_KEY to your TfNSW API key")
    with make_session(key) as session:
        while True:
            collect(session, args.kind, args.output)
            if args.interval is None:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        # Exception text from third-party HTTP libraries may contain request details.
        LOG.error("Ingestion failed (%s). No completion manifest was published for the failed snapshot.", type(exc).__name__)
        raise SystemExit(1)
