# app/frontend/main.py
import os
import json
import time
import uuid
from typing import Dict, Any, List
from flask import Flask, request, Response
from confluent_kafka import Producer, Consumer, KafkaException

# ===== Kafka config from environment =====
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")                # e.g., pkc-xxxx.gcp.confluent.cloud:9092
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")        # Confluent Cloud API key
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")        # Confluent Cloud API secret
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

REQUEST_TOPIC = os.getenv("REQUEST_TOPIC", "requests")
RESPONSE_TOPIC = os.getenv("RESPONSE_TOPIC", "responses")

# Frontend response timeout (seconds). Dataproc jobs can take minutes; default to 10 minutes.
RESP_TIMEOUT_SEC = float(os.getenv("FRONTEND_RESPONSE_TIMEOUT_SEC", "600"))

# Fail fast if critical envs are missing
_required = [BOOTSTRAP, SASL_USERNAME, SASL_PASSWORD]
if any(v in (None, "") for v in _required):
    raise RuntimeError("Missing Kafka envs: KAFKA_BOOTSTRAP, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD")

# ===== Flask app =====
app = Flask(__name__)

# ===== Kafka helpers =====
def build_producer() -> Producer:
    """Create a Confluent Cloud Kafka Producer."""
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanisms": SASL_MECHANISM,
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,
        "message.timeout.ms": 30000,
        "client.id": "frontend-producer",
    }
    return Producer(conf)

def build_consumer(group_id: str) -> Consumer:
    """Create a Confluent Cloud Kafka Consumer."""
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanisms": SASL_MECHANISM,
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "frontend-consumer",
    }
    return Consumer(conf)

def send_and_wait(payload: Dict[str, Any], expected_action: str, timeout_sec: float = RESP_TIMEOUT_SEC) -> Dict[str, Any] | None:
    """Produce request to Kafka and synchronously wait for the matching response."""
    req_id = payload["request_id"]

    # 1) Create consumer first
    consumer = build_consumer(group_id=f"frontend-{req_id}")
    consumer.subscribe([RESPONSE_TOPIC])

    # 2) Then produce the request
    producer = build_producer()
    try:
        producer.produce(
            REQUEST_TOPIC,
            key=req_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        producer.flush(15.0)
    except KafkaException as e:
        try:
            consumer.close()
        except Exception:
            pass
        return {"request_id": req_id, "success": False, "error": f"Producer error: {e}"}

    # 3) Wait for response
    deadline = time.time() + timeout_sec
    match = None
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue
            if data.get("request_id") == req_id and data.get("action") == expected_action:
                match = data
                break
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    return match

# ===== Minimal HTML templates (plain CSS to mimic provided mock-ups) =====
HTML_INDEX = """
<!doctype html>
<html><head><meta charset="utf-8"><title>ScholarMine</title></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto;">
  <h1 style="font-weight:700; font-size:42px; text-align:center;">Enter Google Scholar Search URL</h1>
  <form method="post" action="/build" style="margin-top:36px;">
    <input type="url" name="scholar_url" required
           placeholder="https://scholar.google.com/scholar?q=... site:ieeexplore.ieee.org"
           style="width:100%; font-size:18px; padding:14px; border:1px solid #ccc; border-radius:8px;">
    <div style="height:28px"></div>
    <button type="submit"
            style="display:block; width:100%; padding:22px; font-size:34px; font-weight:700; background:#ddd; border:2px solid #888; border-radius:10px;">
      Index Papers from this URL
    </button>
  </form>
</body></html>
"""

HTML_ACTIONS = """
<!doctype html>
<html><head><meta charset="utf-8"><title>ScholarMine</title></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto;">
  <div style="border:2px solid #aaa; padding:18px; text-align:center; font-size:26px; font-weight:700;">
    Engine was Loaded<br>&amp;<br>Inverted indicies were constructed successfully!
  </div>
  <h2 style="text-align:center; margin-top:36px;">Please Select Action</h2>
  <div style="display:flex; gap:24px; justify-content:center; margin-top:18px;">
    <form method="get" action="/search">
      <input type="hidden" name="rid" value="{rid}">
      <button type="submit" style="font-size:28px; padding:16px 28px; background:#ddd; border:2px solid #888; border-radius:10px;">
        Search for Term
      </button>
    </form>
    <form method="get" action="/topn">
      <input type="hidden" name="rid" value="{rid}">
      <button type="submit" style="font-size:28px; padding:16px 28px; background:#ddd; border:2px solid #888; border-radius:10px;">
        Most Frequent Terms
      </button>
    </form>
  </div>
</body></html>
"""

HTML_SEARCH_FORM = """
<!doctype html>
<html><head><meta charset="utf-8"><title>ScholarMine</title></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto;">
  <h1 style="font-weight:700; font-size:42px; text-align:center;">Enter Your Search Term</h1>
  <form method="post" action="/search" style="margin-top:36px;">
    <input type="hidden" name="rid" value="{rid}">
    <input type="text" name="term" required placeholder="Type your search term here!"
           style="width:100%; font-size:18px; padding:14px; border:1px solid #ccc; border-radius:8px;">
    <div style="height:28px"></div>
    <button type="submit"
            style="display:block; width:100%; padding:22px; font-size:34px; font-weight:700; background:#ddd; border:2px solid #888; border-radius:10px;">
      Search
    </button>
  </form>
</body></html>
"""

def render_doc_table(records: List[Dict[str, Any]]) -> str:
    """Render document rows for both 'topn' and 'search_term' results."""
    if not records:
        body = "<tr><td colspan='4'>No results.</td></tr>"
    else:
        body = "".join(
            f"<tr>"
            f"<td><a target='_blank' href='https://ieeexplore.ieee.org/document/{r['doc_id']}'>{r['doc_id']}</a></td>"
            f"<td>{r['citations']}</td>"
            f"<td>{r['doc_name']}</td>"
            f"<td>{r['freq']}</td>"
            f"</tr>"
            for r in records
        )
    return f"""
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; width:100%; margin-top:20px;">
      <thead style="background:#eee;">
        <tr>
          <th>Doc ID<br><small>In IEEE Xplore Paper URL</small></th>
          <th># of Citations</th>
          <th>Doc Name</th>
          <th>Frequencies</th>
        </tr>
      </thead>
      <tbody>{body}</tbody>
    </table>
    """

def render_topn_table(records: List[Dict[str, Any]]) -> str:
    """Render document rows for both 'topn' and 'search_term' results."""
    if not records:
        body = "<tr><td colspan='4'>No results.</td></tr>"
    else:
        body = "".join(
            f"<tr>"
            f"<td>{r['doc_name']}</td>"
            f"<td>{r['freq']}</td>"
            f"</tr>"
            for r in records
        )
    return f"""
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; width:100%; margin-top:20px;">
      <thead style="background:#eee;">
        <tr>
          <th>Term</th>
          <th>Frequencies</th>
        </tr>
      </thead>
      <tbody>{body}</tbody>
    </table>
    """

# ===== Routes =====
@app.get("/")
def index_page():
    """Landing page: user enters a Scholar URL to build the inverted index."""
    return Response(HTML_INDEX, mimetype="text/html")

@app.post("/build")
def build_index():
    """Submit action=build_index and then redirect user to the actions page."""
    scholar_url = (request.form.get("scholar_url") or "").strip()
    request_id = str(uuid.uuid4())

    payload = {
        "request_id": request_id,
        "action": "build_index",
        "url": scholar_url,
        "ts": int(time.time() * 1000),
    }

    resp = send_and_wait(payload, expected_action="build_index")
    if not resp or not resp.get("success"):
        msg = (resp and resp.get("error")) or "Timed out or unknown error while building index."
        return Response(f"<pre>Indexing failed: {msg}</pre>", status=500, mimetype="text/html")

    return Response(HTML_ACTIONS.format(rid=request_id), mimetype="text/html")

@app.get("/actions")
def actions_page():
    """Actions page shown after index is built successfully."""
    rid = request.args.get("rid", "")
    return Response(HTML_ACTIONS.format(rid=rid), mimetype="text/html")

@app.get("/search")
def search_form():
    """Form to input a search term (uses the existing request_id/index)."""
    rid = request.args.get("rid", "")
    return Response(HTML_SEARCH_FORM.format(rid=rid), mimetype="text/html")

@app.post("/search")
def search_submit():
    """Submit action=search_term; render a document table for matches."""
    rid = request.form.get("rid", "")
    term = (request.form.get("term") or "").strip()

    payload = {
        "request_id": rid,
        "action": "search_term",
        "term": term,
        "ts": int(time.time() * 1000),
    }

    resp = send_and_wait(payload, expected_action="search_term")
    if not resp or not resp.get("success"):
        msg = (resp and resp.get("error")) or "Timed out or unknown error during search."
        return Response(f"<pre>Search failed: {msg}</pre>", status=500, mimetype="text/html")

    records = resp.get("results", [])
    runtime_ms = resp.get("runtime_ms", "n/a")

    html = f"""
    <!doctype html><html><head><meta charset="utf-8"><title>Search Result</title></head>
    <body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto;">
      <h2>You searched for the term: <b>{term}</b></h2>
      <p>Your search was executed in <b>{runtime_ms}</b> ms</p>
      {render_doc_table(records)}
      <p style="margin-top:24px;"><a href="/actions?rid={rid}" style="font-size:20px;">Select Again</a></p>
    </body></html>
    """
    return Response(html, mimetype="text/html")

@app.get("/topn")
def topn_page():
    """Submit action=topn; render a document table for Top-N results."""
    rid = request.args.get("rid", "")
    top_n = int(request.args.get("n", os.getenv("DEFAULT_TOPN", "20")))

    payload = {
        "request_id": rid,
        "action": "topn",
        "top_n": top_n,
        "ts": int(time.time() * 1000),
    }

    resp = send_and_wait(payload, expected_action="topn")
    if not resp or not resp.get("success"):
        msg = (resp and resp.get("error")) or "Timed out or unknown error while computing Top-N."
        return Response(f"<pre>Top-N failed: {msg}</pre>", status=500, mimetype="text/html")

    records = resp.get("results", [])
    runtime_ms = resp.get("runtime_ms", "n/a")

    html = f"""
    <!doctype html><html><head><meta charset="utf-8"><title>Top-N</title></head>
    <body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto;">
      <h2>Most Frequent Terms</h2>
      <p>Your search was executed in <b>{runtime_ms}</b> ms</p>
      {render_topn_table(records)}
      <p style="margin-top:24px;"><a href="/actions?rid={rid}" style="font-size:20px;">Select Again</a></p>
    </body></html>
    """
    return Response(html, mimetype="text/html")

# ===== Entrypoint =====
if __name__ == "__main__":
    # Host and port can be configured via env for containers
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    app.run(host=host, port=port)
