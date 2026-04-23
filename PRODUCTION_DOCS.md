# 🌺 Calyx Production Data Extraction Pipeline (v2.0+)

High-performance, fault-tolerant pipeline for extracting structured angiosperm species data at scale (160,000+ species).

---

## 🚀 Core Architecture

The pipeline is built around **asynchronous, parallel extraction** with strict control over:

- API rate limits
- Fault tolerance
- Resume capability
- Memory efficiency

### 🔑 Data Sources

| Source        | Purpose |
|--------------|--------|
| GBIF         | Taxonomic backbone + species list |
| iNaturalist  | Observations + real-world images |
| Wikipedia    | Species descriptions |
| Wikimedia    | Fallback + supplementary images |

---

## ⚙️ Key System Components

### 1. Hard Checkpoint System (CRITICAL)

Enhanced checkpointing system:

- Tracks:
  - `processed_ids` (species-level)
  - `order_progress`
  - `last_offset` (GBIF pagination)
- Enables:
  - Crash-safe resume
  - Offset continuation (no re-fetching)
  - Long-running jobs (multi-day)

Checkpoint file: calyx_checkpoint.csv

---

### 2. GBIF Streaming Engine

- Uses `offset + limit` pagination
- Filters:
  - Only `ACCEPTED` species
  - Only valid `key` entries
- Avoids:
  - Duplicate processing (checkpoint filter)

Flow: GBIF → batch fetch → filter processed → stream forward

---

### 3. iNaturalist Pipeline (PRIMARY IMAGE SYSTEM)

#### 🔍 Taxon Resolution

Multi-stage query strategy:
1. Exact binomial
2. Normalised name
3. Genus fallback

+ GBIF synonym resolution BEFORE query

---

#### 📸 Image Classification System (NEW)

Images are split into **quality tiers**:

| Tier        | Meaning |
|------------|--------|
| research   | Verified observations (highest quality) |
| needs_id   | Community observations |
| fallback   | Any remaining valid images |

---

#### 🎯 Selection Logic

Priority: research > needs_id > fallback

Max limits:
- `research_images`: 3
- `needs_id_images`: 3
- Total cap enforced downstream

---

#### 🔐 License Filtering (STRICT)

Only allowed: cc0, cc-by, cc-by-sa


Max limits:
- `research_images`: 3
- `needs_id_images`: 3
- Total cap enforced downstream

---

#### 🔐 License Filtering (STRICT)

Only allowed:
    inat_id
    observation_count
    inat_research_images
    inat_needs_id_images
    inat_status


---

#### 🔁 Retry + Backoff System

- 3 retries
- Backoff: `2s → 4s → 8s`
- Early exit on:
  - success
  - valid "no_images"

---

### 4. Wikipedia Text Pipeline

Fallback chain: binomial → species → genus → none


Includes:
- Async retry wrapper
- Cache layer (disk persisted)
- Minimum content length validation

---

### 5. Wikimedia Image System (SECONDARY)

Runs independently of iNat.

#### Pipeline:

Search → Resolve → Score → Rank → Deduplicate → Top N


---

#### 🧠 Image Scoring Model

Factors:

- Resolution (soft weight)
- File type (JPEG preferred)
- Wikimedia source boost
- Metadata match (binomial match)
- Junk penalties:
  - diagram
  - map
  - illustration

---

### 6. Species Processing Pipeline

Per-species flow: GBIF → iNat → Wikipedia → Wikimedia → Merge → Record


Key outputs:

- Structured taxonomy
- Cleaned common name
- Multi-source images
- Description + source tracking

---

### 7. Retry Pass System (NEW CRITICAL FIX)

After each chunk:

- Detects failed species (not checkpointed)
- Re-runs extraction pipeline
- Recovers missed iNat hits

---

### 8. Streaming CSV Writer

- Writes in batches
- Avoids memory blow-up
- Handles header state automatically

---

## 📊 Output Schema

    Key fields:
    order
    family
    genus
    species
    scientific_name
    common_name

    gbif_id
    inat_id
    observation_count

    inat_research_images
    inat_needs_id_images
    wiki_images

    wikipedia_description
    description_source

    inat_status
    has_images
    extraction_timestamp


---

## ⚡ Performance Design

| Feature | Purpose |
|--------|--------|
| asyncio | parallel I/O |
| semaphores | API protection |
| batching | memory control |
| checkpointing | crash recovery |
| retry pass | completeness boost |

---

## 🔥 What Makes This Pipeline Unique

- Handles **160K+ species scale**
- Multi-source **image intelligence system**
- Hard checkpoint resume (offset-aware)
- Intelligent iNat classification
- Dual image pipelines (iNat + Wikimedia)
- Retry recovery layer (rare in pipelines)

---

## ⚠️ Known Constraints

- iNaturalist rate limits → controlled via delay + workers
- Wikipedia inconsistencies → handled via fallback chain
- Some species will always have no images

---

## 🧠 Design Philosophy

> "Never fail the pipeline — degrade gracefully."

---
