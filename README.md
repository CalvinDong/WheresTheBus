# Where's The Bus — initial ingestion

Local ingestion for the Sydney Bus Reliability data engineering project:
**TfNSW → Python → raw Bronze snapshots**. Python 3.10+.

## Run locally

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Try synthetic realtime data first; no API key or network needed.
python ingest.py demo

# Set this locally; don't commit or share your key.
export TFNSW_API_KEY='your-key'
python ingest.py static
python ingest.py realtime

# Optional continuous collection; Ctrl+C stops it.
python ingest.py realtime --interval 60
```

Register an application in the [TfNSW Open Data portal](https://opendata.transport.nsw.gov.au/)
and enable access to **Public Transport - Timetables - For Realtime** and
**Public Transport - Realtime Trip Update**. The client uses `Authorization: apikey <key>`.
Check your application's rate limits before choosing a polling interval.
The static feed should usually run once daily via a scheduler; realtime can run
every minute while collecting history. Each command also works as a one-shot scheduled job.

## Output

```text
data/bronze/
  gtfs_static/date=YYYY-MM-DD/hour=HH/<snapshot_id>/
    feed.zip
    metadata.json
  gtfs_realtime/date=YYYY-MM-DD/hour=HH/<snapshot_id>/
    feed.pb
    feed.json
    metadata.json
```

`--output /your/path` changes the destination. Demo data goes under `data/demo/`.
Partitions and collection timestamps use UTC. Raw ZIP/protobuf files are retained
unchanged so future transformations can replay them. The static bundle may contain
operator ZIPs; this step inventories the outer archive without extracting tables.
It checks ZIP structure, not full GTFS table semantics or nested archive integrity.

Realtime JSON is a readable protobuf decoding, retaining trip-level cancellations,
deleted entities, stop updates, and optional-field presence. Protobuf JSON represents
64-bit integers (including timestamps) as strings; absent fields remain absent.
A missing delay is different from an explicit zero delay.

Each `metadata.json` contains source, timestamps, byte count, SHA-256, and basic feed
information. **Only consume snapshots with a metadata file**: it is written atomically
last as the completion marker. Downloads stream to disk. HTTP 429/transient server
errors and connection failures get bounded retries and backoff; read failures during
streaming fail the run. An interrupted/invalid snapshot can leave diagnostic payloads
without a manifest. Errors exit nonzero; a scheduler can retry or alert. Polling also
stops on failure rather than silently losing observations.

Every successful poll has its own snapshot ID, even when the source bytes repeat.
This preserves collection history; use the SHA-256 for downstream content deduplication.
Re-running creates another snapshot: there is no exactly-once ingestion guarantee.

## What comes next

This is an ingestion starter, not the Silver transformation or Azure deployment.
Next, extract static operator tables and turn realtime entities into typed Parquet
observations, then join using feed/operator identity, trip, service date, and stop
sequence. Keep GTFS service-day times beyond 24:00 and Australia/Sydney timezone
rules in mind. Do not substitute collection date for a missing service date.

Trip updates contain predictions and reported delays. They do **not** establish
actual bus arrivals on their own. Preserve repeated predictions, cancellations,
and skipped stops before defining a reliability metric. No arrival matching or
on-time percentages are calculated here.

## Checks

```sh
python -m unittest discover -s tests -v
```

Tests use synthetic protobufs and mocked HTTP responses. Live TfNSW access requires
your own enabled API key and is not verified by these checks.

References: [TfNSW documentation](https://opendata.transport.nsw.gov.au/developers/documentation),
[TfNSW API authentication example](https://opendataforum.transport.nsw.gov.au/t/code-change-for-programmatic-download-of-gtfs-and-txc-files/436),
[GTFS Python bindings](https://gtfs.org/documentation/realtime/language-bindings/python/),
[GTFS realtime reference](https://gtfs.org/documentation/realtime/reference/).
