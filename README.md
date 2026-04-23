# 🌺 Calyx Pipeline

A fault-tolerant, high-throughput data ingestion pipeline for large-scale plant species extraction.

Built to process **160,000+ angiosperm species**, the Calyx pipeline integrates biodiversity data across multiple sources and transforms it into a structured, analysis-ready dataset.

---

## 🌿 Overview

What started as a simple scraper evolved into a resilient ingestion system designed to handle:

- API rate limits  
- taxonomic inconsistencies  
- missing or incomplete records  
- long-running execution (multi-day jobs)

The pipeline pulls from three primary sources:

| Source | Role |
|-------|------|
| GBIF | Taxonomic backbone |
| iNaturalist | Observations + ecological imagery |
| Wikipedia / Wikimedia | Descriptions + reference images |

---

## ⚙️ Core Features

### 🔁 Hard Checkpointing
- Resume from exact failure point  
- Tracks:
  - processed species IDs  
  - GBIF offsets  
  - per-order progress  

---

### ⚡ Async + Parallel Processing
- Built on `asyncio` + `aiohttp`
- Controlled concurrency via semaphores
- Designed for high throughput without triggering API limits

---

### 🧠 Intelligent Taxonomic Handling
- Binomial extraction (Genus + species)
- Synonym resolution via GBIF match API
- Reduces `taxon_not_found` errors across APIs

---

### 📸 Dual Image System (Decoupled)

#### iNaturalist (Primary)
- Real-world observations
- Image classification:
  - `research` (high confidence)
  - `needs_id` (moderate confidence)
  - `fallback` (low confidence)

#### Wikimedia (Secondary)
- Reference-quality images
- Metadata-driven scoring system:
  - resolution weighting
  - file type preference
  - species name matching
  - soft filtering (no hard rejection)

---

### 🔁 Retry + Backoff Strategy
- Exponential backoff with jitter
- Handles:
  - timeouts  
  - HTTP 429 (rate limiting)  
- Prevents pipeline collapse under load

---

### 🔄 Post-Run Retry Pass
- Reprocesses failed species automatically
- Improves dataset completeness without full reruns

---

### 💾 Streaming CSV Output
- Batch-based append writes
- Prevents memory overload
- Scales cleanly beyond 100K+ rows

---

## 🧬 Pipeline Flow


GBIF → Clean → Resolve → iNaturalist → Wikipedia → Wikimedia → Merge → CSV → Checkpoint


---

## 📊 Output Schema

Key fields:

- Taxonomy: `order`, `family`, `genus`, `species`
- IDs: `gbif_id`, `inat_id`
- Media:
  - `inat_research_images`
  - `inat_needs_id_images`
  - `wiki_images`
- Metadata:
  - `common_name`
  - `observation_count`
  - `inat_status`
- Text:
  - `wikipedia_description`
  - `description_source`

---

## 🌍 Scaling Strategy

Species are processed in **taxonomic batches (orders)**:

```python
BATCH_ORDER_NAMES = {
    1: ["Asparagales", "Liliales"],
    2: ["Poales", "Arecales", "Alismatales"],
    ...
}

This:

reduces API pressure
isolates failures
improves recovery
🚀 Running the Pipeline
Basic run
python calyx_production.py --batch 1 --limit 5000
Resume run
python calyx_production.py --batch 1 --resume
🐳 Docker

Build:

docker build -t calyx-pipeline .

Run:

docker run -it -v $(pwd)/data:/app calyx-pipeline
🧠 Design Philosophy

Build for failure, not for success.

The pipeline prioritises:

graceful degradation
recoverability
completeness over perfection
📈 Current State
Stable across long runs
Handles large taxonomic datasets
Still evolving
🔮 Next Steps
downstream recognition model integration
dataset validation layer
multi-node scaling
🤝 Feedback

This is an evolving system.

If you're working with large biological datasets or ingestion pipelines — would love to hear how you'd push this further.