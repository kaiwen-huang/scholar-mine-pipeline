# app/worker/worker.py
import os
import json
import time
import uuid
import certifi
from typing import List, Dict

from confluent_kafka import Consumer, Producer
from google.cloud import dataproc_v1
from google.cloud import storage
from scholar_fetch import fetch_ieee_papers_from_scholar

# ===== Env =====
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

REQUEST_TOPIC = os.getenv("REQUEST_TOPIC", "requests")
RESPONSE_TOPIC = os.getenv("RESPONSE_TOPIC", "responses")

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION", "us-central1")
CLUSTER_NAME = os.getenv("CLUSTER_NAME", "scholarmine-dataproc")
BUCKET_NAME = os.getenv("BUCKET_NAME")

STREAMING_JAR = os.getenv(
    "HADOOP_STREAMING_JAR",
    "file:///usr/lib/hadoop/hadoop-streaming.jar",
)

# ===== Kafka builders =====
def build_consumer() -> Consumer:
    """Create Kafka consumer connected to Confluent Cloud."""
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanisms": SASL_MECHANISM,
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,
        "ssl.ca.location": certifi.where(),
        "group.id": f"worker-group-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
    }
    return Consumer(conf)

def build_producer() -> Producer:
    """Create Kafka producer for sending responses."""
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanisms": SASL_MECHANISM,
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,
        "ssl.ca.location": certifi.where(),
    }
    return Producer(conf)

# ===== GCS helpers =====
_storage = storage.Client()

def load_request_metadata(request_id: str) -> Dict[str, Dict]:
    """Load per-request metadata written to input/{request_id}/abstracts.jsonl by gcs_write_input_jsonl.

    This returns a dict mapping doc_id -> {doc_name, citations} parsed from the JSONL produced
    when the worker uploaded the scraped papers. If not present, returns empty dict.
    """
    if not BUCKET_NAME:
        return {}
    bucket = _storage.bucket(BUCKET_NAME)
    blob = bucket.blob(f"input/{request_id}/abstracts.jsonl")
    if not blob.exists():
        return {}
    text = blob.download_as_text()
    meta_map: Dict[str, Dict] = {}
    for ln in [l.strip() for l in text.splitlines() if l.strip()]:
        try:
            obj = json.loads(ln)
            doc_id = str(obj.get("doc_id") or obj.get("id") or obj.get("docid"))
            if not doc_id:
                continue
            meta_map[doc_id] = {
                "doc_name": obj.get("doc_name") or obj.get("title") or "N/A",
                "citations": int(obj.get("citations") or 0),
            }
        except Exception:
            continue
    return meta_map

def gcs_write_input_jsonl(request_id: str, scholar_url: str = None) -> None:
    """If scholar_url is provided, fetch papers and upload to gs://BUCKET_NAME/input/{request_id}/abstracts.jsonl.

    If scholar_url is None, function will check whether the blob already exists and do nothing if present.
    This avoids unintentional fetches when the input was pre-uploaded.
    """
    bucket = _storage.bucket(BUCKET_NAME)
    blob = bucket.blob(f"input/{request_id}/abstracts.jsonl")

    if scholar_url:
        papers = fetch_ieee_papers_from_scholar(scholar_url, max_pages=5, max_papers=50)

        lines = []
        for p in papers:
            lines.append(json.dumps({
                "doc_id": p.doc_id,
                "doc_name": p.doc_name,
                "citations": p.citations,
                "abstract": p.abstract,
                "ieee_url": p.ieee_url,
            }))

        blob.upload_from_string("\n".join(lines), content_type="application/json")
    else:
        # If no scholar_url provided, assume input was pre-uploaded. If not present, warn and return.
        if not blob.exists():
            print(f"Input blob gs://{BUCKET_NAME}/input/{request_id}/abstracts.jsonl not found and no scholar_url provided")
            return

def gcs_list_text(prefix: str) -> List[str]:
    """Read all non-empty 'part-*' files under a GCS prefix and return lines."""
    bucket = _storage.bucket(BUCKET_NAME)
    lines: List[str] = []
    for b in bucket.list_blobs(prefix=prefix):
        name = b.name.rsplit("/", 1)[-1]
        if not name.startswith("part-"):
            continue
        content = b.download_as_text().strip()
        if content:
            lines.extend([ln for ln in content.splitlines() if ln.strip()])
    return lines

def parse_topn_lines(lines: List[str]) -> List[Dict]:
    """Parse reducer_topn output.

    Expected (recommended) format per line:
        doc_id,citations,doc_name,freq

    Fallback: if line has 2 columns (term count) -> coerce to doc_name=fake row.
    """
    out: List[Dict] = []
    for ln in lines:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) >= 4:
            doc_id, citations, doc_name, freq = parts[:4]
            try:
                out.append({
                    "doc_id": doc_id,
                    "citations": int(citations),
                    "doc_name": doc_name,
                    "freq": int(freq),
                })
            except ValueError:
                continue
        else:
            # Fallback for "term count" format: "<term>\t<count>" or "term count"
            term, cnt = None, None
            if "\t" in ln:
                xs = ln.split("\t")
            else:
                xs = ln.split()
            if len(xs) == 2:
                term, cnt = xs[0].strip(), xs[1].strip()
            if term and cnt and cnt.isdigit():
                out.append({
                    "doc_id": "-",
                    "citations": 0,
                    "doc_name": term,
                    "freq": int(cnt),
                })
    # Sort by freq desc
    out.sort(key=lambda r: r["freq"], reverse=True)
    return out

def parse_index_for_term(request_id: str, term: str) -> List[Dict]:
    """Read inverted index and return docs for a term with frequencies per doc."""
    prefix = f"index/{request_id}/inverted/"
    lines = gcs_list_text(prefix)
    term_l = term.lower()
    results: Dict[str, int] = {}
    for ln in lines:
        # Format from reducer_invert.py: "<term>\tdoc1,doc1,doc2,..."
        if "\t" not in ln:
            continue
        w, docs_csv = ln.split("\t", 1)
        if w.strip().lower() != term_l:
            continue
        for doc_id in [d.strip() for d in docs_csv.split(",") if d.strip()]:
            results[doc_id] = results.get(doc_id, 0) + 1

    # Load per-request metadata first (from input/{request_id}/abstracts.jsonl).
    req_meta = load_request_metadata(request_id)
    # request-specific metadata is the only source now
    meta_map = req_meta
    rows = []
    for doc_id, freq in results.items():
        meta = meta_map.get(doc_id, {})
        rows.append({
            "doc_id": doc_id,
            "citations": int(meta.get("citations", 0)),
            "doc_name": meta.get("doc_name", "N/A"),
            "freq": freq,
        })
    rows.sort(key=lambda r: r["freq"], reverse=True)
    return rows

# ===== Dataproc jobs =====
def submit_job(job: dict) -> str:
    """Submit a Dataproc job and wait for completion; return job_id."""
    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    op = client.submit_job_as_operation(
        request={"project_id": PROJECT_ID, "region": REGION, "job": job}
    )
    res = op.result()  # wait
    return res.reference.job_id

def job_build_index(request_id: str, gcs_in: str) -> str:
    """Run Hadoop Streaming job to build inverted index."""
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "hadoop_job": {
            "main_jar_file_uri": STREAMING_JAR,
            "args": [
                "-files", ",".join([
                    f"gs://{BUCKET_NAME}/jobs/mapper_invert.py",
                    f"gs://{BUCKET_NAME}/jobs/reducer_invert.py",
                ]),
                "-mapper", "python3 mapper_invert.py",
                "-reducer", "python3 reducer_invert.py",
                "-input",  gcs_in,
                "-output", f"gs://{BUCKET_NAME}/index/{request_id}/inverted",
            ],
        },
    }
    return submit_job(job)

def job_word_count(request_id: str, gcs_in: str) -> str:
    """Run word count using mapper_ii/reducer_ii (intermediate for topN)."""
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "hadoop_job": {
            "main_jar_file_uri": STREAMING_JAR,
            "args": [
                "-files", ",".join([
                    f"gs://{BUCKET_NAME}/jobs/stopwords.txt",
                    f"gs://{BUCKET_NAME}/jobs/mapper_ii.py",
                    f"gs://{BUCKET_NAME}/jobs/reducer_ii.py",
                ]),
                "-mapper", "python3 mapper_ii.py",
                "-reducer", "python3 reducer_ii.py",
                "-input",  gcs_in,
                "-output", f"gs://{BUCKET_NAME}/tmp/{request_id}/ii",
            ],
        },
    }
    return submit_job(job)

def job_topn(request_id: str, top_n: int) -> str:
    """Run topN using mapper_topn/reducer_topn and return job id."""
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "hadoop_job": {
            "main_jar_file_uri": STREAMING_JAR,
            "args": [
                "-files", ",".join([
                    f"gs://{BUCKET_NAME}/jobs/mapper_topn.py",
                    f"gs://{BUCKET_NAME}/jobs/reducer_topn.py",
                ]),
                "-mapper",  "python3 mapper_topn.py",
                "-reducer", "python3 reducer_topn.py",
                "-input",   f"gs://{BUCKET_NAME}/tmp/{request_id}/ii",
                "-output",  f"gs://{BUCKET_NAME}/output/{request_id}/topn",
                "-cmdenv",  f"TOP_N={top_n}",
            ],
        },
    }
    return submit_job(job)

# ===== Action handlers =====
def handle_build_index(payload: dict) -> dict:
    """Action: build_index -> run only the index job and return success."""
    request_id = payload["request_id"]
    scholar_url = payload.get("url", "")
    gcs_write_input_jsonl(request_id, scholar_url)
    
    gcs_in = f"gs://{BUCKET_NAME}/input/{request_id}/abstracts.jsonl"
    t0 = time.time()
    job_id = job_build_index(request_id, gcs_in)
    runtime_ms = int((time.time() - t0) * 1000)

    return {
        "request_id": request_id,
        "success": True,
        "action": "build_index",
        "runtime_ms": runtime_ms,
        "job_ids": [job_id],
        "gcs_index": f"gs://{BUCKET_NAME}/index/{request_id}/inverted",
    }

def handle_topn(payload: dict) -> dict:
    """Action: topn -> run word count + topn and parse results."""
    request_id = payload["request_id"]
    top_n = int(payload.get("top_n", 20))

    # Ensure input exists (placeholder upload)
    gcs_write_input_jsonl(request_id)
    gcs_in = f"gs://{BUCKET_NAME}/input/{request_id}/abstracts.jsonl"

    t0 = time.time()
    job1 = job_word_count(request_id, gcs_in)
    job2 = job_topn(request_id, top_n)
    runtime_ms = int((time.time() - t0) * 1000)

    # Read and parse topN output
    lines = gcs_list_text(prefix=f"output/{request_id}/topn/")
    results = parse_topn_lines(lines)

    # Enrich results using per-request metadata from input/{request_id}/abstracts.jsonl
    meta_map = load_request_metadata(request_id)
    for r in results:
        doc_id = r.get("doc_id")
        if doc_id and doc_id in meta_map:
            m = meta_map[doc_id]
            r.setdefault("doc_name", m.get("doc_name", "N/A"))
            r.setdefault("citations", int(m.get("citations", 0)))

    return {
        "request_id": request_id,
        "success": True,
        "action": "topn",
        "runtime_ms": runtime_ms,
        "job_ids": [job1, job2],
        "gcs_output": f"gs://{BUCKET_NAME}/output/{request_id}/topn",
        "results": results,
    }

def handle_search_term(payload: dict) -> dict:
    """Action: search_term -> read inverted index and return docs for the term."""
    request_id = payload["request_id"]
    term = (payload.get("term") or "").strip()
    if not term:
        return {"request_id": request_id, "success": False, "error": "Missing term"}

    t0 = time.time()
    results = parse_index_for_term(request_id, term)
    runtime_ms = int((time.time() - t0) * 1000)

    return {
        "request_id": request_id,
        "success": True,
        "action": "search_term",
        "runtime_ms": runtime_ms,
        "results": results,
    }

# ===== Main loop =====
def main():
    consumer = build_consumer()
    producer = build_producer()
    consumer.subscribe([REQUEST_TOPIC])

    print(f"Worker listening on topic: {REQUEST_TOPIC}")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"Invalid JSON message: {e}")
            continue

        req_id = payload.get("request_id", "unknown")
        action = payload.get("action", "topn")
        print(f"Received request_id={req_id} action={action}")

        try:
            if action == "build_index":
                resp = handle_build_index(payload)
            elif action == "search_term":
                resp = handle_search_term(payload)
            else:  # default to topN
                resp = handle_topn(payload)
        except Exception as e:
            resp = {"request_id": req_id, "success": False, "error": str(e)}

        try:
            producer.produce(
                RESPONSE_TOPIC,
                key=req_id.encode("utf-8"),
                value=json.dumps(resp).encode("utf-8"),
            )
            producer.flush(10)
            print(f"Sent response for request_id={req_id}")
        except Exception as e:
            print(f"Failed to send response: {e}")

if __name__ == "__main__":
    main()
