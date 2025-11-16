# ** Scholar Mine Pipeline **

## **1. Overview**

This project is a full end-to-end cloud-based MapReduce search engine built on Google Cloud Dataproc, Kafka, Flask, and Hadoop Streaming.
Given a Google Scholar search URL, the system automatically scrapes IEEE papers, builds an inverted index, performs distributed word count, and supports term-based search and Top-N analysis.

This project implements **Course Project Option 2** with all required features:

* Distributed inverted index using Hadoop Streaming
* Distributed word count + Top-N
* Frontend UI for user interaction
* Kafka for asynchronous communication
* Worker service for job orchestration
* Google Cloud Dataproc cluster (provisioned with Terraform)
* GCS as data storage
* Complete demo video and code walkthrough video

---

## **2.1. System Architecture**

### **Main Structure**
```
app/
  frontend/        # Flask web interface; provides the UI, handles user input,
                   # sends requests to Kafka, and displays results returned from backend.

  worker/          # Backend service that consumes Kafka requests, fetches IEEE papers,
                   # uploads abstracts to GCS, submits Dataproc MapReduce jobs,
                   # reads outputs, and sends responses back to Kafka.

  jobs/            # All Hadoop Streaming mapper/reducer scripts (inverted index,
                   # word count, top-N, plus stop-word list). These are uploaded
                   # to GCS and executed by Dataproc.

dataproc/          # Infrastructure-as-Code: provisions Dataproc cluster, GCS bucket,
                   # service accounts, IAM policies, and all cloud resources required
                   # by the system.

demo.mp4           # Functional demonstration video (≥5 minutes).
                   # Shows UI, requests, Dataproc indexing, search, and Top-N results.

code.mp4           # Full code walkthrough video (≥10 minutes).
                   # Explains frontend, worker, Kafka flow, Terraform, and all mappers/reducers.

README.md          # Project documentation: setup, architecture, commands, workflow,
                   # Terraform usage, job descriptions, demo instructions, and citations.

```
### **High-level diagram**

```
Frontend → Kafka → Worker → Dataproc → GCS → Worker → Kafka → Frontend
```

### **Full workflow**

1. User submits request through the frontend
2. Frontend sends request → Kafka (`requests` topic)
3. Worker consumes request
4. Worker scrapes IEEE papers and uploads metadata to GCS
5. Worker submits Dataproc MapReduce jobs
6. Hadoop Streaming executes mapper/reducer
7. Results are written back to GCS
8. Worker reads results and publishes response → Kafka (`responses` topic)
9. Frontend receives the response and renders the result page

---

# **2.2. Technology Stack**

This project integrates cloud infrastructure, distributed systems, and containerized web services.
All major technologies used are listed below.

### **Frontend**

* **Flask (Python)**
  Lightweight web framework providing UI, request handling, and display of results.
* **HTML / CSS**
  Simple UI for submitting queries and rendering search / Top-N tables.
* **Docker**
  Containerizes the entire frontend for reproducible deployment.

### **Backend Worker**

* **Python Worker Service**
  Consumes Kafka messages, orchestrates GCS uploads, triggers Dataproc jobs,
  reads MapReduce output, and responds back to Kafka.
* **Google Cloud Storage (GCS)**
  Stores:

  * input abstracts (`input/{request_id}/abstracts.jsonl`)
  * inverted index outputs
  * Top-N outputs
  * uploaded Hadoop Streaming scripts

### **MapReduce / Dataproc**

* **Google Cloud Dataproc**
  Managed Hadoop cluster executing all MapReduce jobs.
* **Hadoop Streaming**
  Mapper/Reducer scripts written in Python:

  * `mapper_invert.py` and `reducer_invert.py`
  * `mapper_ii.py` and `reducer_ii.py` (word count)
  * `mapper_topn.py` and `reducer_topn.py`
* **Stop-word List**
  Uploaded to GCS and referenced by word count scripts.

### **Message Queue**

* **Confluent Cloud Kafka**

  * Request topic (`requests`)
  * Response topic (`responses`)
* Messages trigger:

  * index building
  * search query
  * Top-N query

### **Infrastructure**

* **Terraform**

  * Automates deployment of Dataproc clusters, service accounts, IAM bindings, and GCS buckets.
  * Ensures reproducible cloud infrastructure.

---

# **2.3. Prerequisites**

Before deploying or running this project, ensure the following tools, accounts, and permissions are available:

### **Local Requirements**

* Python 3.9+
* Docker & Docker Compose
* Google Cloud SDK (`gcloud`)
* Terraform ≥ 1.3
* `gsutil` (installed through Google Cloud SDK)
* A Unix-like shell (macOS / Linux recommended)

### **Google Cloud Requirements**

* A GCP project (billing enabled)
* IAM permissions:
  * Compute Admin
  * Dataproc Admin
  * Storage Admin
  * Service Account Admin
* A Dataproc-compatible region (e.g., `us-central1`)
* A GCS bucket for:
  * Hadoop Streaming scripts (`jobs/`)
  * Input abstracts
  * Inverted index output
  * Top-N output

### **Kafka (Confluent Cloud) Requirements**

* Confluent Cloud account
* Kafka cluster + API key/secret
* Two topics:
  * `requests`
  * `responses`

### **Environment Variables**

Both the frontend and worker require a `.env` containing:

```
KAFKA_BOOTSTRAP=
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
REQUEST_TOPIC=requests
RESPONSE_TOPIC=responses

PROJECT_ID=
REGION=us-central1
CLUSTER_NAME=
BUCKET_NAME=
HADOOP_STREAMING_JAR=file:///usr/lib/hadoop/hadoop-streaming.jar
```

---

## **3. Components**

### **3.1 Frontend (Flask)**

Located at: `app/frontend/`

Responsibilities:

* Accept Google Scholar URL
* Trigger inverted index construction
* Search for individual terms
* Run Top-N frequent term analysis
* Communicate asynchronously via Kafka

Run locally:

```bash
docker build -t scholar-frontend ./app/frontend
docker run -p 8080:8080 --env-file ./app/frontend/.env scholar-frontend
```

---

### **3.2 Worker**

Located at: `app/worker/`

Responsibilities:

* Consume Kafka requests
* Scrape IEEE papers using `scholar_fetch`
* Write abstracts to GCS as JSONL
* Submit Dataproc MapReduce jobs
* Read computation results
* Publish response back to Kafka

Worker handles three actions:

* `build_index`
* `search_term`
* `topn`

Run locally:

```bash
cd app/worker
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)

python worker.py
```

---

### **3.3 MapReduce Jobs**

Located at: `app/jobs/`

Jobs included:

| Purpose        | Mapper             | Reducer             |
| -------------- | ------------------ | ------------------- |
| Inverted Index | `mapper_invert.py` | `reducer_invert.py` |
| Word Count     | `mapper_ii.py`     | `reducer_ii.py`     |
| Top-N          | `mapper_topn.py`   | `reducer_topn.py`   |

Upload jobs to GCS:

```bash
gsutil cp app/jobs/*.py gs://scholarmine-dataproc-bucket/jobs/
gsutil cp app/jobs/stopwords.txt gs://scholarmine-dataproc-bucket/jobs/
```

---

### **3.4 Dataproc Cluster (Terraform)**

Located at: `dataproc/terraform/`

Includes:

* Cluster provisioning
* GCS buckets
* Service account configuration

Deploy with:

```bash
terraform init
terraform apply -auto-approve
```

---

## **4. Running the System**

### **Step 1 — Deploy Dataproc (Terraform)**

```bash
cd dataproc/terraform
terraform init
terraform apply -auto-approve
```

### **Step 2 — Upload MapReduce Jobs to GCS**

```bash
gsutil cp app/jobs/*.py gs://<YOUR_BUCKET>/jobs/
gsutil cp app/jobs/stopwords.txt gs://<YOUR_BUCKET>/jobs/
```

### **Step 3 — Start Worker**

```bash
python3 app/worker/worker.py
```

(Worker uses Kafka credentials from `.env`)

### **Step 4 — Build & Run Frontend**

```bash
docker build -t scholar-frontend ./app/frontend
docker run -p 8080:8080 --env-file ./app/frontend/.env scholar-frontend
```

### **Step 5 — Use the Web UI**

1. Enter Google Scholar search URL
2. Build inverted index
3. Select one:

   * Search Term
   * Top-N Terms

---

## **5. Data Flow Summary**

### **Input**

Raw abstracts collected by the worker are written to:

```
gs://<BUCKET>/input/<request_id>/abstracts.jsonl
```

This file contains all documents used by the MapReduce jobs.

---

### **Word Count Output (Intermediate)**

Preprocessed tokens for inverted index construction:

```
gs://<BUCKET>/tmp/<request_id>/ii/
```

Used only as the input for the inverted index job.

---

### **Inverted Index Output**

Final inverted index mapping terms → document IDs:

```
gs://<BUCKET>/index/<request_id>/inverted/
```

Used by the worker to answer keyword search queries.

---

### **Top-N Output**

Top-N most frequent terms (based on total occurrences):

```
gs://<BUCKET>/output/<request_id>/topn/
```

Returned to the frontend as part of the search results.

---

## **6. Demo Video Requirements**

Per course specifications:

### **1. Code Walkthrough Video**

* **Minimum 10 minutes**
* Must walk through:

  * Frontend
  * Worker
  * All MapReduce Scripts
  * Dataproc submission flow
  * Terraform infrastructure

### **2. Functional Demo Video**

* **Minimum 5 minutes**
* Must demonstrate:

  * Scholar URL ingestion
  * Index build
  * Term search
  * Top-N analysis

---

## **7. Troubleshooting**

### **Kafka shows no response in frontend**

Cause: Consumer group ID mismatch.
Fix: The frontend uses a per-request consumer group, so ensure no old consumer is stuck.

### **Top-N result empty**

Cause: Mapper_topn originally filtered out all records.
Fix: Rewrote mapper to pass through `term count` lines.

### **Dataproc job fails**

Common causes:

* mapper/reducer not uploaded
* missing stopwords file
* wrong bucket path

---

## **8. Citation Requirement**

As required, debugging assistance for this project made use of OpenAI ChatGPT:

**Citation:**

> Parts of debugging, architecture clarification, and MapReduce troubleshooting were assisted using *ChatGPT 5.1 (OpenAI, 2025)*.

---

## **9. Other Assumptions**

1. **Google Scholar Results Contain IEEE Xplore Links**
   The system assumes that the Google Scholar URL provided by the user includes IEEE Xplore documents.
   Only those papers are fetched and indexed, as the metadata parser is designed specifically for IEEE pages.

2. **Dataproc Cluster Has Sufficient Compute Resources**
   Default cluster configuration (1 master, 2 workers) is assumed to be sufficient for the expected dataset size
   (≤ 50 papers, each with one abstract).
   Larger data volumes may require a larger cluster.

3. **Input Abstract File Format is Stable**
   The worker uploads a JSONL file in the format:

   ```
   {"doc_id": "...", "doc_name": "...", "citations": <int>, "abstract": "..."}
   ```

   All MapReduce jobs assume this format remains unchanged.

4. **Hadoop Streaming Environment Has Python 3 Available**
   All mapper/reducer scripts require Python 3 on Dataproc nodes.
   This is available on Dataproc by default.

5. **Kafka Topics Already Exist**
   The two required Kafka topics (`requests`, `responses`) are assumed to be created via Terraform prior to running the system.
