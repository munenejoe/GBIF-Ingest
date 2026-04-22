"""
CALYX PRODUCTION DATA EXTRACTION PIPELINE
==========================================
High-performance, fault-tolerant plant species data extraction system.

Features:
- Parallel processing with asyncio + aiohttp
- Automatic checkpointing and resume capability
- Smart Wikipedia fallback chain with caching
- Rate limiting and retry logic
- Memory-efficient streaming writes
- Comprehensive logging

Author: Calyx Data Team
Version: 2.0 Production
"""

import asyncio
import aiohttp
import requests
import pandas as pd
import json
import logging
import argparse
import sys
import re
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

GBIF_SPECIES_URL = "https://api.gbif.org/v1/species/search"
GBIF_MATCH_URL = "https://api.gbif.org/v1/species/match"
WIKI_API_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/"
GBIF_BACKBONE_KEY = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"
INAT_API_URL = "https://api.inaturalist.org/v1/taxa"

# Taxonomic orders organized by batch
BATCH_ORDER_NAMES = {
    1: ["Asparagales", "Liliales"],
    2: ["Poales", "Arecales", "Alismatales"],
    3: ["Asterales"],
    4: ["Lamiales", "Gentianales"],
    5: ["Solanales", "Ericales", "Apiales"],
    6: ["Rosales", "Fabales"],
    7: ["Malpighiales", "Myrtales"],
    8: ["Brassicales", "Sapindales", "Malvales"],
    9: ["Magnoliales", "Ranunculales", "Caryophyllales"],
}

# Performance tuning
MAX_CONCURRENT_GBIF = 5      # Concurrent GBIF requests
MAX_CONCURRENT_WIKI = 3     # Concurrent Wikipedia requests
GBIF_BATCH_SIZE = 100        # Species per GBIF request
CHECKPOINT_INTERVAL = 500    # Save checkpoint every N species
REQUEST_TIMEOUT = 15         # Seconds
MAX_RETRIES = 3              # Retry failed requests

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class SpeciesRecord:
    """Structured species data record."""
    order: str
    family: str
    genus: str
    species: str
    scientific_name: str
    common_name: str
    gbif_id: int
    inat_id: Optional[str]
    inat_status: Optional[str]  # "success", "no_images", "taxon_not_found", etc.
    has_images: bool
    observation_count: Optional[int]
    wikipedia_description: str
    description_source: str  # "species", "cleaned", "genus", "none"
    extraction_timestamp: str
    image_urls: str  # now JSON: [{"url": "...", "source": "inat|wiki"}]

    def to_dict(self):
        """Convert to dictionary for CSV writing."""
        return asdict(self)

# ============================================================================
# BASIC SCHEMA VALIDATION
# ============================================================================

class SchemaField:
    def __init__(self, expected_type, default=None, required=False, transform=None):
        self.expected_type = expected_type
        self.default = default
        self.required = required
        self.transform = transform


class Schema:
    def __init__(self, fields: Dict[str, SchemaField]):
        self.fields = fields

    def validate(self, data: Optional[Dict]) -> Dict:
        """Validate and normalize incoming data safely."""
        data = data or {}
        clean = {}

        for key, field in self.fields.items():
            value = data.get(key, field.default)

            # Type enforcement
            if not isinstance(value, field.expected_type):
                value = field.default

            # Transform if needed
            if field.transform:
                try:
                    value = field.transform(value)
                except Exception:
                    value = field.default

            # Required field check
            if field.required and value is None:
                value = field.default

            clean[key] = value

        return clean

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """Configure structured logging to console and optional file."""
    logger = logging.getLogger("CalyxExtractor")
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

class CheckpointManager:
    """Manages checkpointing for resumable extraction."""
    
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = Path(checkpoint_file)
        self.processed_ids: Set[int] = set()
        self.order_progress: Dict[str, int] = defaultdict(int)
        self.load()
    
    def load(self):
        """Load existing checkpoint if available."""
        if self.checkpoint_file.exists():
            logger.info(f"📂 Loading checkpoint: {self.checkpoint_file}")
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.processed_ids = set(data.get('processed_ids', []))
                    self.order_progress = defaultdict(int, data.get('order_progress', {}))
                logger.info(f"✅ Loaded {len(self.processed_ids)} processed species")
            except Exception as e:
                logger.warning(f"⚠️  Failed to load checkpoint: {e}")
    
    def save(self):
        """Save current checkpoint state."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({
                    'processed_ids': list(self.processed_ids),
                    'order_progress': dict(self.order_progress),
                    'last_update': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")
    
    def mark_processed(self, gbif_id: int, order: str):
        """Mark a species as processed."""
        self.processed_ids.add(gbif_id)
        self.order_progress[order] += 1
    
    def is_processed(self, gbif_id: int) -> bool:
        """Check if species already processed."""
        return gbif_id in self.processed_ids
    
    def get_order_count(self, order: str) -> int:
        """Get number of processed species for order."""
        return self.order_progress[order]

# ============================================================================
# WIKIPEDIA CACHE
# ============================================================================

class WikipediaCache:
    """Thread-safe Wikipedia response cache with disk persistence."""
    
    def __init__(self, cache_file: str = "wiki_cache.json"):
        self.cache_file = Path(cache_file)
        self.cache: Dict[str, Tuple[str, str]] = {}  # name -> (description, source)
        self.load()
    
    def load(self):
        """Load cache from disk."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
                logger.info(f"📦 Loaded Wikipedia cache: {len(self.cache)} entries")
            except Exception as e:
                logger.warning(f"⚠️  Failed to load cache: {e}")
    
    def save(self):
        """Save cache to disk."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save cache: {e}")
    
    def get(self, name: str) -> Optional[Tuple[str, str]]:
        """Get cached result."""
        return self.cache.get(name)
    
    def set(self, name: str, description: str, source: str):
        """Cache a result."""
        self.cache[name] = (description, source)

# ============================================================================
# GBIF ORDER KEY RESOLUTION
# ============================================================================

def sync_backbone_keys(order_names: List[str]) -> Dict[str, int]:
    """
    Synchronously resolve order names to GBIF Backbone keys.
    This is done once at startup, so sync is acceptable.
    """
    verified_orders = {}
    logger.info(f"🔍 Resolving {len(order_names)} order keys from GBIF Backbone...")
    
    for name in order_names:
        params = {
            "name": name,
            "rank": "ORDER",
            "kingdom": "Plantae",
            "strict": True
        }
        
        try:
            response = requests.get(GBIF_MATCH_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if (
                data.get("matchType") != "NONE"
                and data.get("kingdom") == "Plantae"
                and data.get("rank") == "ORDER"
            ):
                order_key = data.get("orderKey")
                verified_orders[name] = order_key
                logger.info(f"  ✅ {name}: {order_key}")
            else:
                logger.warning(f"  ⚠️  Could not resolve {name}")
                
        except Exception as e:
            logger.error(f"  ❌ Error resolving {name}: {e}")
        
        time.sleep(0.2)  # Be polite to API
    
    return verified_orders

# ============================================================================
# ASYNC GBIF FETCHER
# ============================================================================

max_offset = 10000  # safety cap to prevent infinite loops

async def fetch_gbif_species(
    session: aiohttp.ClientSession,
    order_name: str,
    order_key: int,
    limit: int,
    checkpoint: CheckpointManager
) -> List[Dict]:
    """
    Fetch species from GBIF with safe pagination + checkpoint resume.
    """

    logger.info(f"🌸 Fetching species for {order_name} (Key: {order_key})")

    all_species = []

    # ✅ DEFINE FIRST (fixes your error)
    batch_size = GBIF_BATCH_SIZE
    max_offset = 20000  # safety cap

    # ✅ Resume from checkpoint safely
    already_processed = checkpoint.get_order_count(order_name)

    if already_processed > 0:
        logger.info(f"  ↻ Resuming from {already_processed} processed species")

    # Align offset to batch boundary
    offset = (already_processed // batch_size) * batch_size

    while len(all_species) < limit:

        # 🚨 SAFETY STOP
        if offset > max_offset:
            logger.warning(f"  🧯 Offset limit reached ({max_offset}) — stopping")
            break

        params = {
            "highertaxonKey": order_key,
            "datasetKey": GBIF_BACKBONE_KEY,
            "rank": "SPECIES",
            "status": "ACCEPTED",
            "limit": batch_size,
            "offset": offset
        }

        try:
            async with session.get(
                GBIF_SPECIES_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as response:

                if response.status != 200:
                    logger.warning(f"  ⚠️ GBIF returned {response.status} at offset {offset}")
                    break

                data = await response.json()
                results = data.get("results", [])

                # ✅ No more pages
                if not results:
                    logger.info(f"  ✓ No more results at offset {offset}")
                    break

                # ✅ Filter already processed
                new_results = [
                    r for r in results
                    if isinstance(r.get("key"), int)
                    and not checkpoint.is_processed(r["key"])
                ]

                # 🚨 CRITICAL: prevent infinite scan
                if not new_results:
                    logger.info(f"  🛑 No new species at offset {offset} — stopping")
                    break

                all_species.extend(new_results)

                logger.info(
                    f"  → Fetched {len(new_results)} new species (offset: {offset})"
                )

                offset += batch_size

                # Stop if we hit limit
                if len(all_species) >= limit:
                    break

                await asyncio.sleep(0.2)

        except asyncio.TimeoutError:
            logger.warning(f"  ⏱️ Timeout at offset {offset}")
            break

        except Exception as e:
            logger.error(f"  ❌ Error at offset {offset}: {e}")
            break

    logger.info(f"✅ Fetched {len(all_species)} species from {order_name}")

    return all_species[:limit]

async def fetch_gbif_common_name(session, gbif_id: int) -> str:
    """Fetch best common name (prefer English)."""
    
    url = f"https://api.gbif.org/v1/species/{gbif_id}/vernacularNames"

    try:
        async with session.get(url) as response:
            if response.status != 200:
                return ""

            data = await response.json()
            results = data.get("results", [])

            if not results:
                return ""

            # ✅ Prefer English
            for r in results:
                if r.get("language") == "eng":
                    return r.get("vernacularName", "")

            # Fallback: first available
            return results[0].get("vernacularName", "")

    except Exception as e:
        print(f"[GBIF COMMON NAME ERROR] {gbif_id}: {e}")
        return ""

async def resolve_gbif_synonym(session, name: str) -> Optional[str]:
    """
    Resolve GBIF scientific name to accepted canonical name.
    Fixes synonym drift before hitting iNaturalist.
    """

    if not name:
        return None

    params = {
        "name": name,
        "strict": False,
        "verbose": True
    }

    try:
        async with session.get(
            GBIF_MATCH_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

            # ✅ accepted usage (best case)
            if data.get("usageKey") and data.get("status") == "ACCEPTED":
                return data.get("canonicalName")

            # 🔁 synonym → use accepted name
            if data.get("synonym") and data.get("acceptedUsage"):
                return data["acceptedUsage"].get("canonicalName")

            # fallback
            return data.get("canonicalName")

    except Exception:
        return None

# ============================================================================
# DATA CLEANING & NORMALIZATION
# ============================================================================

def fallback_inat(reason="unknown"):
    return {
        "inat_id": None,
        "observations": None,
        "images": [],
        "common_name": "",
        "inat_status": reason
    }

def clean_common_name(name: str) -> str:
    if not name:
        return ""
    return name.strip().lower().capitalize()

def extract_binomial(name: str) -> Optional[str]:
    """
    Extract proper Genus + species (binomial) from GBIF scientific name.
    Handles authorship, subspecies, hybrids, etc.
    """
    if not name:
        return None

    # Remove hybrid markers like ×
    name = name.replace("×", " ").strip()

    # Regex for Genus species (first valid binomial)
    match = re.match(r"^([A-Z][a-zA-Z-]+)\s+([a-z-]+)", name)
    
    if match:
        genus, species = match.groups()
        return f"{genus} {species}"

    return None

# ============================================================================
# ASYNC INATURALIST FETCHER
# ============================================================================

inat_queue = asyncio.Queue()
REQUEST_TIMEOUT = 25  # was 15

INAT_WORKERS = 2      # was 3
INAT_DELAY = 2.0      # was 1.2

MAX_INITIAL_IMAGES = 2   # Stage 1
MAX_TOTAL_IMAGES = 3     # Stage 2 ONLY

INAT_TIMEOUT = aiohttp.ClientTimeout(total=40)

def normalize_inat_query(name: str) -> Optional[str]:
    """
    Normalize scientific name for iNaturalist search.
    - Removes authorities
    - Removes hybrid markers
    - Extracts binomial
    """

    if not name:
        return None

    # Remove hybrid symbols
    name = name.replace("×", " ").replace(" x ", " ").strip()

    # Extract Genus species
    match = re.match(r"^([A-Z][a-zA-Z-]+)\s+([a-z-]+)", name)

    if match:
        genus, species = match.groups()
        return f"{genus} {species}"

    return None

async def fetch_inat_data(session, scientific_name: str):
    """
    Production-grade iNaturalist fetch:
    - GBIF synonym resolution
    - clean binomial extraction
    - multi-strategy search
    - soft matching
    """

    HEADERS = {
        "User-Agent": "CalyxDataBot/2.0 (contact: joemetha97@gmail.com)"
    }

    # =========================
    # 🔥 STEP 1: CLEAN NAME
    # =========================
    clean_name = normalize_inat_query(scientific_name)

    if not clean_name:
        return fallback_inat("invalid_name")

    # =========================
    # 🔥 STEP 2: SYNONYM RESOLVE (BIG WIN)
    # =========================
    resolved_name = await resolve_gbif_synonym(session, clean_name)

    if resolved_name:
        clean_name = resolved_name

    # =========================
    # 🔍 SEARCH STRATEGIES
    # =========================
    queries = [
        clean_name,
        clean_name.replace("-", " "),
        clean_name.split()[0]  # genus fallback
    ]

    try:
        taxon = None

        for q in queries:

            params = {
                "q": q,
                "per_page": 5
            }

            async with session.get(
                INAT_API_URL,
                params=params,
                headers=HEADERS,
                timeout=INAT_TIMEOUT
            ) as response:

                if response.status == 429:
                    await asyncio.sleep(5 + random.uniform(1, 3))
                    continue

                if response.status != 200:
                    continue

                data = await response.json()
                results = data.get("results", [])

                if not results:
                    continue

                # ✅ soft match first
                for r in results:
                    name_match = r.get("name", "").lower()
                    if clean_name.lower() in name_match:
                        taxon = r
                        break

                # fallback: first result
                if not taxon:
                    taxon = results[0]

                if taxon:
                    break

        if not taxon:
            return fallback_inat("taxon_not_found")

        taxon_id = taxon.get("id")

        await asyncio.sleep(0.5)

        # =========================
        # 📸 OBSERVATIONS
        # =========================
        obs_url = "https://api.inaturalist.org/v1/observations"
        obs_params = {
            "taxon_id": taxon_id,
            "photos": "true",
            "per_page": 3
        }

        image_urls = []

        async with session.get(
            obs_url,
            params=obs_params,
            headers=HEADERS,
            timeout=INAT_TIMEOUT
        ) as obs_response:

            if obs_response.status == 429:
                await asyncio.sleep(5 + random.uniform(1, 3))
                return fallback_inat("rate_limited")

            if obs_response.status != 200:
                return fallback_inat("obs_failed")

            obs_data = await obs_response.json()

            for obs in obs_data.get("results", []):
                for photo in obs.get("photos", []):
                    url = photo.get("url")
                    if url:
                        image_urls.append(url.replace("square", "medium"))

                    if len(image_urls) >= MAX_INITIAL_IMAGES:
                        break
                if len(image_urls) >= MAX_INITIAL_IMAGES:
                    break

        # =========================
        # 📸 TAXON PHOTO FALLBACK
        # =========================
        if not image_urls:
            for photo in taxon.get("taxon_photos", []):
                url = photo.get("photo", {}).get("medium_url")
                if url:
                    image_urls.append(url)

                if len(image_urls) >= MAX_INITIAL_IMAGES:
                    break

        return {
            "inat_id": taxon_id,
            "observations": taxon.get("observations_count"),
            "images": image_urls,
            "common_name": (
                taxon.get("preferred_common_name")
                or taxon.get("english_common_name")
                or ""
            ),
            "inat_status": "success" if image_urls else "no_images"
        }

    except Exception:
        return fallback_inat("exception")

SEM = asyncio.Semaphore(5)  # tune: 5–10 safe

async def process_row(session, df, idx):
    async with SEM:
        row = df.iloc[idx]

        try:
            images = json.loads(row["image_urls"])
        except:
            images = []

        if len(images) >= MAX_TOTAL_IMAGES or not row["inat_id"]:
            return None

        try:
            expanded = await expand_images(
                session,
                int(row["inat_id"]),
                images
            )

            return idx, json.dumps(expanded[:MAX_TOTAL_IMAGES])

        except:
            return None


async def enrich_images(csv_path, output_path="enriched.csv"):
    df = pd.read_csv(csv_path)

    connector = aiohttp.TCPConnector(limit=20)
    timeout = aiohttp.ClientTimeout(total=40)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        tasks = [
            process_row(session, df, idx)
            for idx in range(len(df))
        ]

        completed = 0

        for future in asyncio.as_completed(tasks):
            result = await future

            if result:
                idx, images_json = result
                df.at[idx, "image_urls"] = images_json

            completed += 1

            # ✅ checkpoint (FIXED)
            if completed % 100 == 0:
                df.to_csv(output_path, index=False)
                print(f"💾 Saved at {completed}")

    df.to_csv(output_path, index=False)
    print("✅ Enrichment complete")

async def expand_images(session, taxon_id: int, existing_images: List[str]) -> List[str]:
    """
    Slow, controlled image expansion.
    Max total images = 3
    """

    if len(existing_images) >= MAX_TOTAL_IMAGES:
        return existing_images

    await asyncio.sleep(1.5)  # 🔥 HARD THROTTLE

    url = "https://api.inaturalist.org/v1/observations"
    params = {
        "taxon_id": taxon_id,
        "photos": "true",
        "per_page": 10
    }

    images = list(existing_images)

    try:
        async with session.get(url, params=params, timeout=INAT_TIMEOUT) as res:
            if res.status != 200:
                return images

            data = await res.json()

            for obs in data.get("results", []):
                for photo in obs.get("photos", []):
                    url = photo.get("url")
                    if url:
                        clean = url.replace("square", "medium")

                        if clean not in images:
                            images.append(clean)

                    if len(images) >= MAX_TOTAL_IMAGES:
                        return images

    except Exception:
        return images

    return images
    
def parse_inat_response(data: Dict) -> Dict:
    """
    Parse iNaturalist API response into normalized structure.
    Handles:
    - taxon extraction
    - observation images
    - fallback to taxon photos
    """

    try:
        results = data.get("results", [])
        if not results:
            return fallback_inat()

        taxon = results[0]
        taxon_id = taxon.get("id")

        images = []

        # 🔥 Taxon photos (safe fallback)
        for photo in taxon.get("taxon_photos", []):
            url = photo.get("photo", {}).get("medium_url")
            if url:
                images.append(url)

            if len(images) >= 5:
                break

        return {
            "inat_id": taxon_id,
            "observations": taxon.get("observations_count"),
            "images": images[:5],
            "common_name": (
                taxon.get("preferred_common_name")
                or taxon.get("english_common_name")
                or ""
            )
        }

    except Exception as e:
        print(f"[PARSE ERROR] {e}")
        return fallback_inat()

async def inat_worker(session, result_store, stop_event):
    while not stop_event.is_set():
        try:
            species_key, scientific_name, future = await inat_queue.get()

            try:
                result = await fetch_inat_data(session, scientific_name)

                if not isinstance(result, dict):
                    result = fallback_inat("invalid_response")

                result_store[species_key] = result

                # ✅ FIX: check before setting
                if not future.done():
                    future.set_result(result)

            except Exception:
                result = fallback_inat("worker_exception")

                if not future.done():
                    future.set_result(result)

            finally:
                inat_queue.task_done()
                await asyncio.sleep(INAT_DELAY)

        except asyncio.CancelledError:
            break

# ============================================================================
# WIKIPEDIA FETCHER WITH SMART FALLBACK
# ============================================================================

async def fetch_wikipedia_description(
    session: aiohttp.ClientSession,
    species_name: str,
    genus: str,
    cache: WikipediaCache
) -> Tuple[str, str]:
    """
    Fetch Wikipedia description with smart fallback chain.
    Returns: (description, source)
    
    Fallback chain:
    1. Full species name
    2. Cleaned species name (remove subspecies notation)
    3. Genus name
    4. None
    """
    # Check cache first
    cached = cache.get(species_name)
    if cached:
        return cached
    
    timeout = aiohttp.ClientTimeout(total=8)

    async def try_fetch(name: str) -> Optional[str]:
        headers = {
            "User-Agent": "CalyxBot/1.0 (your_email@example.com)"
        }

        try:
            # STEP 1: Direct summary endpoint
            url = WIKI_API_URL + name.replace(" ", "_")

            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    extract = data.get("extract")

                    if extract and len(extract) > 50:
                        return extract

            # STEP 2: Search fallback
            search_url = "https://en.wikipedia.org/w/api.php"
            params = {
                "action": "query",
                "list": "search",
                "srsearch": name,
                "format": "json"
            }

            async with session.get(search_url, params=params, headers=headers, timeout=timeout) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                results = data.get("query", {}).get("search", [])

                if results:
                    top_title = results[0]["title"]

                    url = WIKI_API_URL + top_title.replace(" ", "_")

                    async with session.get(url, headers=headers, timeout=timeout) as page_res:
                        if page_res.status == 200:
                            page_data = await page_res.json()
                            extract = page_data.get("extract")

                            if extract and len(extract) > 50:
                                return extract

        except Exception as e:
            print(f"[WIKI ERROR] {name}: {e}")  # <-- DEBUG VISIBILITY

        return None
    
    # Try 1: Extract true binomial (Genus species)
    binomial = extract_binomial(species_name)

    if binomial and binomial != species_name:
        desc = await try_fetch(binomial)
        if desc:
            cache.set(species_name, desc, "binomial")
            return desc, "binomial"

    # Try 2: Full species name
    desc = await try_fetch(species_name)
    if desc:
        cache.set(species_name, desc, "species")
        return desc, "species"

    # Try 3: Genus Fallback
    if genus:
        desc = await try_fetch(genus)
        if desc:
            cache.set(species_name, desc, "genus")
            return desc, "genus"
    
    # Try 4: None found
    return "Description not found.", "none"

# =========================
#   RANKING FUNCTION
# =========================

def score_image(url: str, info: Dict, species_name: str) -> float:
    """
    Improved ranking system for Wikimedia images.
    No hard filtering — pure scoring.
    """

    score = 0.0

    # =========================
    # 1. TRUE RESOLUTION (FIXED)
    # =========================
    width = info.get("width", 0)
    height = info.get("height", 0)

    if width and height:
        megapixels = (width * height) / 1_000_000
        score += min(megapixels / 2.0, 2.5)  # stronger signal than before

    # =========================
    # 2. FILE QUALITY SIGNAL
    # =========================
    if "upload.wikimedia.org" in url:
        score += 0.8

    # =========================
    # 3. FILE TYPE BONUS
    # =========================
    if url.lower().endswith((".jpg", ".jpeg")):
        score += 0.6
    elif url.lower().endswith(".png"):
        score += 0.3

    # =========================
    # 4. TEXT RELEVANCE (WEAK SIGNAL ONLY)
    # =========================
    description = (info.get("extmetadata", {})
                   .get("ImageDescription", {})
                   .get("value", "")).lower()

    categories = (info.get("extmetadata", {})
                  .get("Categories", {})
                  .get("value", "")).lower()

    binomial = extract_binomial(species_name)
    if binomial:
        bn = binomial.lower()
        if bn in description:
            score += 1.8
        if bn in categories:
            score += 1.2

    # =========================
    # 5. PENALISE NON-PHOTO CONTENT
    # =========================
    junk_terms = ["diagram", "map", "chart", "logo", "icon", "drawing", "illustration"]

    if any(k in description for k in junk_terms):
        score -= 1.5

    if any(k in categories for k in junk_terms):
        score -= 1.0

    return score


# =========================
# 🌿 MAIN FUNCTION (FIXED)
# =========================

async def fetch_wikimedia_images(session, species_name: str) -> List[str]:
    """
    Clean Wikimedia Commons-first pipeline:
    - searches File namespace directly
    - no premature filtering
    - scores everything properly
    """

    API = "https://en.wikipedia.org/w/api.php"

    headers = {
        "User-Agent": "CalyxBot/2.0 (contact: youremail@example.com)"
    }

    # =========================
    # STEP 1: SEARCH COMMONS FILES (IMPORTANT FIX)
    # =========================
    async def search_commons(name: str) -> List[str]:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": name,
            "srnamespace": 6,  # FILE namespace ONLY
            "format": "json"
        }

        async with session.get(API, params=params, headers=headers) as res:
            if res.status != 200:
                return []

            data = await res.json()
            results = data.get("query", {}).get("search", [])

            return [r["title"] for r in results if "title" in r]

    # =========================
    # STEP 2: RESOLVE IMAGEINFO (CORRECT SOURCE OF TRUTH)
    # =========================
    async def resolve_files(titles: List[str]) -> List[Dict]:
        results = []

        for title in titles[:30]:

            params = {
                "action": "query",
                "titles": title,
                "prop": "imageinfo",
                "iiprop": "url|size|extmetadata",
                "format": "json"
            }

            try:
                async with session.get(API, params=params, headers=headers) as res:
                    if res.status != 200:
                        continue

                    data = await res.json()
                    pages = data.get("query", {}).get("pages", {})

                    for page in pages.values():
                        info = page.get("imageinfo", [])
                        if not info:
                            continue

                        i = info[0]

                        results.append({
                            "url": i.get("url"),
                            "width": i.get("width", 0),
                            "height": i.get("height", 0),
                            "extmetadata": i.get("extmetadata", {})
                        })

            except Exception:
                continue

        return results

    # =========================
    # STEP 3: MAIN FLOW
    # =========================
    try:
        titles = await search_commons(species_name)

        if not titles:
            return []

        raw_images = await resolve_files(titles)

        if not raw_images:
            return []

        # =========================
        # STEP 4: SCORE EVERYTHING
        # =========================
        scored = []

        for img in raw_images:
            if not img["url"]:
                continue

            s = score_image(img["url"], img, species_name)
            scored.append((s, img["url"]))

        # =========================
        # STEP 5: SORT + PICK TOP N
        # =========================
        scored.sort(reverse=True, key=lambda x: x[0])

        seen = set()
        final = []

        for _, url in scored:
            clean = url.split("?")[0]

            if clean not in seen:
                seen.add(clean)
                final.append(clean)

            if len(final) >= 3:
                break

        return final

    except Exception as e:
        print(f"[WIKI IMG ERROR] {species_name}: {e}")
        return []


async def fetch_wiki_images_strict(session, species_name: str) -> Tuple[List[str], str]:

    WIKI_API = "https://en.wikipedia.org/w/api.php"

    headers = {
        "User-Agent": "CalyxBot/2.0 (contact: youremail@example.com)"
    }

    async def get_page_images(title: str) -> List[str]:
        params = {
            "action": "query",
            "titles": title.replace(" ", "_"),
            "prop": "images",
            "imlimit": 20,
            "format": "json"
        }

        async with session.get(WIKI_API, params=params, headers=headers) as res:
            if res.status != 200:
                return []

            data = await res.json()
            pages = data.get("query", {}).get("pages", {})

            for page in pages.values():
                return [img["title"] for img in page.get("images", [])]

        return []

    async def search_fallback(name: str) -> Optional[str]:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": name,
            "format": "json"
        }

        async with session.get(WIKI_API, params=params, headers=headers) as res:
            if res.status != 200:
                return None

            data = await res.json()
            results = data.get("query", {}).get("search", [])

            if results:
                return results[0]["title"]

        return None

    async def resolve_images(image_titles: List[str]) -> Tuple[List[str], Dict]:
        urls = []
        debug = {"total": 0, "low_res": 0, "bad_keyword": 0, "accepted": 0}

        for title in image_titles[:20]:
            params = {
                "action": "query",
                "titles": title,
                "prop": "imageinfo",
                "iiprop": "url|extmetadata",
                "format": "json"
            }

            try:
                async with session.get(WIKI_API, params=params, headers=headers) as res:
                    if res.status != 200:
                        continue

                    data = await res.json()
                    pages = data.get("query", {}).get("pages", {})

                    for page in pages.values():
                        info = page.get("imageinfo", [])
                        if not info:
                            continue

                        debug["total"] += 1

                        meta = info[0].get("extmetadata", {})
                        url = info[0].get("url")

                        if not url:
                            continue

                        width = int(meta.get("ImageWidth", {}).get("value", 0))
                        height = int(meta.get("ImageHeight", {}).get("value", 0))

                        if width < 400 or height < 300:
                            debug["low_res"] += 1
                            continue

                        description = meta.get("ImageDescription", {}).get("value", "").lower()

                        if any(k in description for k in ["diagram", "map", "chart", "logo"]):
                            debug["bad_keyword"] += 1
                            continue

                        if url.lower().endswith((".jpg", ".jpeg", ".png")):
                            urls.append(url)
                            debug["accepted"] += 1

                        if len(urls) >= 5:
                            return urls, debug

            except Exception:
                continue

        return urls, debug

    # =========================
    # ✅ MAIN EXECUTION (FIXED INDENTATION)
    # =========================
    try:
        image_titles = await get_page_images(species_name)

        if not image_titles:
            alt_title = await search_fallback(species_name)
            if alt_title:
                image_titles = await get_page_images(alt_title)

        if not image_titles:
            return [], "no_titles"

        image_urls, debug = await resolve_images(image_titles)

        print(
            f"[WIKI DEBUG] {species_name} | "
            f"total={debug['total']} "
            f"accepted={debug['accepted']} "
            f"low_res={debug['low_res']} "
            f"bad={debug['bad_keyword']}"
        )

        if image_urls:
            return image_urls, "success"

        if debug["total"] == 0:
            return [], "no_images_found"

        return [], f"filtered_out({debug})"

    except Exception as e:
        print(f"[WIKI IMG ERROR] {species_name}: {e}")
        return [], "exception"

    return [], "unknown"
        
    
# ============================================================================
#   IMAGE JSON MERGE LOGIC
# ============================================================================

def merge_images(inat_images: List[str], wiki_images: List[str]) -> List[str]:
    """Strict merge with priority + dedupe"""

    seen = set()
    merged = []

    # iNat always first
    for url in inat_images:
        clean = url.split("?")[0]
        if clean not in seen:
            seen.add(clean)
            merged.append(clean)

    # Only add wiki if exists
    for url in wiki_images:
        clean = url.split("?")[0]
        if clean not in seen:
            seen.add(clean)
            merged.append(clean)

        if len(merged) >= 5:
            break

    return merged

# ============================================================================
# ASYNC RETRY LOGIC
# ============================================================================

async def retry_async(
    func,
    *args,
    retries=3,
    base_delay=0.5,
    max_delay=5,
    jitter=True,
    validator=None,   # 🔥 NEW
    expected_type=None,  # 🔥 NEW
    **kwargs
):
    """
    Advanced async retry with:
    - Type validation
    - Custom success criteria
    - Exponential backoff + jitter
    """

    for attempt in range(retries):
        try:
            result = await func(*args, **kwargs)

            # ✅ TYPE CHECK (if specified)
            if expected_type and not isinstance(result, expected_type):
                result_valid = False
            else:
                result_valid = True

            # ✅ CUSTOM VALIDATOR (if provided)
            if validator:
                try:
                    result_valid = result_valid and validator(result)
                except Exception:
                    result_valid = False

            # ✅ DEFAULT FALLBACK VALIDATION
            if not validator and not expected_type:
                result_valid = result is not None

            if result_valid:
                return result

        except Exception:
            if attempt == retries - 1:
                return None

        # ⏳ Backoff
        delay = min(max_delay, base_delay * (2 ** attempt))
        if jitter:
            delay *= random.uniform(0.7, 1.3)

        await asyncio.sleep(delay)

    return None


# ============================================================================
#  RECORD COMPLETENESS CHECK
# ============================================================================

def is_record_complete(inat_data, description, images):
    return (
        len(inat_data.get("images", [])) > 0 or
        (description and description != "Description not found.") or
        bool(inat_data.get("common_name"))
    )

def normalize_wiki_result(result):
    if isinstance(result, tuple):
        return {
            "description": result[0],
            "source": result[1]
        }
    return {}

# ============================================================================
# SPECIES PROCESSOR
# ============================================================================

async def process_species(
    species_data: Dict,
    order_name: str,
    session: aiohttp.ClientSession,
    wiki_cache: WikipediaCache
) -> SpeciesRecord:

    canonical_name = species_data.get("canonicalName") or ""
    genus = species_data.get("genus") or ""
    species_key = species_data.get("key")

    binomial = extract_binomial(canonical_name) or canonical_name

    # =========================
    # iNaturalist (queued)
    # =========================
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    await inat_queue.put((species_key, binomial, future))

    MAX_INAT_RETRIES = 3

    inat_data = None

    for attempt in range(MAX_INAT_RETRIES):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        await inat_queue.put((species_key, binomial, future))

        try:
            inat_data = await asyncio.wait_for(future, timeout=25)

            status = inat_data.get("inat_status")

            # ✅ SUCCESS → break immediately
            if status == "success":
                break

            # 🔁 RETRY conditions
            if status in ["timeout", "rate_limited"]:
                await asyncio.sleep(2 * (attempt + 1))
                continue

            # ❌ NON-RETRYABLE
            break

        except asyncio.TimeoutError:
            if attempt < MAX_INAT_RETRIES - 1:
                await asyncio.sleep(2 * (attempt + 1))
                continue
            else:
                inat_data = fallback_inat("timeout")

    # fallback safety
    if not isinstance(inat_data, dict):
        inat_data = fallback_inat("invalid")

    # =========================
    # Wikipedia DESCRIPTION (FIXED)
    # =========================
    desc_res = await retry_async(
        fetch_wikipedia_description,
        session, canonical_name, genus, wiki_cache,
        expected_type=tuple,
        validator=lambda r: r and len(r[0]) > 50
    )

    if isinstance(desc_res, tuple):
        description, source = desc_res
    else:
        description, source = "Description not found.", "none"

    # =========================
    # Wikipedia IMAGES
    # =========================
    wiki_result = await retry_async(
        fetch_wiki_images_strict,
        session, canonical_name,
        expected_type=tuple
    )

    if isinstance(wiki_result, tuple):
        wiki_images, wiki_reason = wiki_result
    else:
        wiki_images, wiki_reason = [], "invalid"

    # =========================
    # IMAGE MERGE (FIXED: USE YOUR FUNCTION)
    # =========================
    merged_urls = merge_images(
        inat_data.get("images", []),
        wiki_images
    )

    final_images = [
        {
            "url": url,
            "source": "inat" if url in inat_data.get("images", []) else "wiki"
        }
        for url in merged_urls[:MAX_TOTAL_IMAGES]
    ]

    has_images = len(final_images) > 0

    # =========================
    # DEBUG (clean + useful)
    # =========================
    print(
        f"[MERGE] {canonical_name} | "
        f"inat={len(inat_data.get('images', []))} "
        f"wiki={len(wiki_images)} "
        f"final={len(final_images)} "
        f"reason={wiki_reason}"
    )

    # =========================
    # BUILD RECORD
    # =========================
    name_parts = canonical_name.split()
    species = name_parts[1] if len(name_parts) > 1 else ""

    return SpeciesRecord(
        order=order_name,
        family=species_data.get("family") or "",
        genus=genus,
        species=species,
        scientific_name=species_data.get("scientificName") or "",
        common_name=clean_common_name(
            inat_data.get("common_name") or species_data.get("vernacularName") or ""
        ),
        gbif_id=species_key or 0,
        inat_id=inat_data.get("inat_id"),
        observation_count=inat_data.get("observations"),
        wikipedia_description=description,        # ✅ FIXED
        description_source=source,                # ✅ FIXED
        image_urls=json.dumps(final_images),
        extraction_timestamp=datetime.now().isoformat(),
        inat_status=inat_data.get("inat_status", "unknown"),
        has_images=has_images
    )

# ============================================================================
# CSV WRITER (STREAMING)
# ============================================================================

class StreamingCSVWriter:
    def __init__(self, output_file: str):
        self.output_file = Path(output_file)

        # ✅ Determine upfront if file already has data
        self.header_written = (
            self.output_file.exists() and self.output_file.stat().st_size > 0
        )

    def write_batch(self, records: List[SpeciesRecord]):
        if not records:
            return

        df = pd.DataFrame([r.to_dict() for r in records])

        # ✅ Always append, control header explicitly
        df.to_csv(
            self.output_file,
            mode='a',
            index=False,
            header=not self.header_written
        )

        self.header_written = True

def analyze_csv(path: str):
    df = pd.read_csv(path)

    print("\n📊 DATASET ANALYSIS")
    print("=" * 50)

    print(f"Total rows: {len(df)}")

    if "inat_status" in df.columns:
        print("\n🔎 iNat Status Breakdown:")
        print(df["inat_status"].value_counts(dropna=False))

    if "has_images" in df.columns:
        coverage = df["has_images"].mean() * 100
        print(f"\n🖼️ Image Coverage: {coverage:.2f}%")

    print("\n📉 Missing Images:")
    print((df["has_images"] == False).sum())

    print("\n📊 Observation Stats:")
    if "observation_count" in df.columns:
        print(df["observation_count"].describe())

    print("\n🔥 Top Failure Reasons:")
    if "inat_status" in df.columns:
        print(df[df["inat_status"] != "success"]["inat_status"].value_counts())

# ============================================================================
# MAIN EXTRACTION ORCHESTRATOR
# ============================================================================

async def extract_order_parallel(
    order_name: str,
    order_key: int,
    limit_per_order: int,
    checkpoint: CheckpointManager,
    wiki_cache: WikipediaCache,
    csv_writer: StreamingCSVWriter
):

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Order: {order_name}")
    logger.info(f"{'='*60}")

    connector = aiohttp.TCPConnector(limit=50)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        species_list = await fetch_gbif_species(
            session, order_name, order_key, limit_per_order, checkpoint
        )

        if not species_list:
            logger.warning(f"⚠️ No species found for {order_name}")
            return

        logger.info(f"🔄 Processing {len(species_list)} species")

        # 🔥 shared result store
        inat_results = {}

        # 🔥 start workers
        stop_event = asyncio.Event()

        workers = [
            asyncio.create_task(inat_worker(session, inat_results, stop_event))
            for _ in range(INAT_WORKERS)
        ]

        processed_count = 0
        wiki_semaphore = asyncio.Semaphore(MAX_CONCURRENT_WIKI)

        async def process_with_semaphore(sp):
            async with wiki_semaphore:
                return await process_species(
                    sp,
                    order_name,
                    session,
                    wiki_cache,
                )

        chunk_size = 100

        for i in range(0, len(species_list), chunk_size):
            chunk = species_list[i:i + chunk_size]

            tasks = [process_with_semaphore(sp) for sp in chunk]
            records = await asyncio.gather(*tasks, return_exceptions=True)

            valid_records = [r for r in records if isinstance(r, SpeciesRecord)]

            csv_writer.write_batch(valid_records)

            for r in valid_records:
                checkpoint.mark_processed(r.gbif_id, order_name)

            processed_count += len(valid_records)

            if processed_count % CHECKPOINT_INTERVAL == 0:
                checkpoint.save()
                wiki_cache.save()
                logger.info(f"💾 Checkpoint saved: {processed_count}")

            logger.info(f"✓ {processed_count}/{len(species_list)}")

            # 🔥 COOL DOWN (prevents API burst detection)
            await asyncio.sleep(1)

            # 🧊 COOL DOWN BEFORE RETRY (PREVENT API HAMMERING)
            await asyncio.sleep(5)

            # =========================
            # 🔁 RETRY PASS (CRITICAL FIX)
            # =========================

            logger.info("🔁 Starting retry pass for failed iNat records...")

            retry_species = [
                sp for sp in species_list
                if isinstance(sp.get("key"), int)
                and not checkpoint.is_processed(sp["key"])
            ]

            if retry_species:
                logger.info(f"🔄 Retrying {len(retry_species)} failed species")

                retry_tasks = [
                    process_with_semaphore(sp)
                    for sp in retry_species
                ]

                retry_results = await asyncio.gather(*retry_tasks, return_exceptions=True)

                retry_valid = [
                    r for r in retry_results
                    if isinstance(r, SpeciesRecord)
                ]

                csv_writer.write_batch(retry_valid)

                for r in retry_valid:
                    checkpoint.mark_processed(r.gbif_id, order_name)

                logger.info(f"✅ Retry recovered {len(retry_valid)} species")
            else:
                logger.info("✅ No retry needed")

            # 🔥 COOL DOWN (prevents API burst detection)
            await asyncio.sleep(1)

        # 🔥 wait for queue to drain
        await inat_queue.join()

        stop_event.set()

        for w in workers:
            w.cancel()

        checkpoint.save()
        wiki_cache.save()

        logger.info(f"✅ Completed {order_name}: {processed_count}")

# ============================================================================
# BATCH EXTRACTION
# ============================================================================

async def extract_batch(
    batch_num: int,
    limit_per_order: int,
    checkpoint: CheckpointManager,
    wiki_cache: WikipediaCache,
    csv_writer: StreamingCSVWriter
):
    """Extract all orders in a batch sequentially (orders in parallel would overload APIs)."""
    
    if batch_num not in BATCH_ORDER_NAMES:
        logger.error(f"❌ Invalid batch number: {batch_num}")
        return
    
    order_names = BATCH_ORDER_NAMES[batch_num]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"🌿 BATCH {batch_num}: {len(order_names)} orders")
    logger.info(f"Orders: {', '.join(order_names)}")
    logger.info(f"{'='*80}\n")
    
    # Resolve order keys
    order_keys = sync_backbone_keys(order_names)
    
    if not order_keys:
        logger.error("❌ No order keys resolved. Aborting batch.")
        return
    
    # Extract each order
    for order_name, order_key in order_keys.items():
        await extract_order_parallel(
            order_name, order_key, limit_per_order,
            checkpoint, wiki_cache, csv_writer
        )

        
        # Pause between orders
        await asyncio.sleep(2)

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Calyx Production Data Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python calyx_production.py --batch 1 --limit 5000
  python calyx_production.py --orders Asterales Rosales --limit 10000
  python calyx_production.py --all --limit 50000
  python calyx_production.py --batch 1 --limit 5000 --resume
        """
    )

    parser.add_argument('--batch', type=int, choices=range(1, 10))
    parser.add_argument('--orders', nargs='+')
    parser.add_argument('--all', action='store_true')

    # 👇 change default to None so we can control it
    parser.add_argument('--limit', type=int, default=None)

    parser.add_argument('--output', type=str, default='calyx_species_data.csv')
    parser.add_argument('--checkpoint', type=str, default='calyx_checkpoint.json')
    parser.add_argument('--log-file', type=str)
    parser.add_argument('--resume', action='store_true')

    args = parser.parse_args()

    if not any([args.batch, args.orders, args.all]):
        parser.print_help()
        print("\n❌ Error: Specify --batch, --orders, or --all")
        sys.exit(1)

    # =========================
    # 🔥 SMART LIMIT HANDLING
    # =========================
    if args.limit is None:
        if args.resume:
            args.limit = 200000
            print("♻️ Resume detected → using high limit: 200,000")
        else:
            args.limit = 50000
            print("🚀 Default run → using safe limit: 50,000")

    # Setup logging
    global logger
    logger = setup_logging(args.log_file)

    # Init components
    checkpoint = CheckpointManager(args.checkpoint)
    wiki_cache = WikipediaCache()
    csv_writer = StreamingCSVWriter(args.output)

    logger.info("🌺 CALYX PRODUCTION DATA EXTRACTION PIPELINE 🌺")
    logger.info(f"Output: {args.output}")
    logger.info(f"Checkpoint: {args.checkpoint}")
    logger.info(f"Limit per order: {args.limit:,}")
    logger.info(f"Max concurrent Wikipedia: {MAX_CONCURRENT_WIKI}")

    start_time = time.time()

    # =========================
    # ✅ ASYNC EXECUTION LAYER
    # =========================

    async def run_all_batches():
        for batch_num in range(1, 10):
            try:
                logger.info(f"\n🚀 Starting batch {batch_num}")
                await extract_batch(
                    batch_num, args.limit, checkpoint, wiki_cache, csv_writer
                )
            except Exception as e:
                logger.error(f"❌ Batch {batch_num} failed: {e}", exc_info=True)

    async def run_single_batch():
        await extract_batch(
            args.batch, args.limit, checkpoint, wiki_cache, csv_writer
        )

    async def run_custom_orders():
        order_keys = sync_backbone_keys(args.orders)
        for order_name, order_key in order_keys.items():
            try:
                await extract_order_parallel(
                    order_name, order_key, args.limit,
                    checkpoint, wiki_cache, csv_writer
                )
            except Exception as e:
                logger.error(f"❌ Order {order_name} failed: {e}")

    # =========================
    # ✅ EXECUTION SWITCH
    # =========================

    try:
        if args.all:
            asyncio.run(run_all_batches())

        elif args.batch:
            asyncio.run(run_single_batch())

        elif args.orders:
            asyncio.run(run_custom_orders())

        # Final saves
        checkpoint.save()
        wiki_cache.save()

        elapsed = (time.time() - start_time) / 60

        logger.info(f"\n{'='*80}")
        logger.info("✨ EXTRACTION COMPLETE!")
        logger.info(f"{'='*80}")
        logger.info(f"📁 Output: {args.output}")
        logger.info(f"⏱️  Time: {elapsed:.2f} minutes")
        logger.info(f"✅ Success!")

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrupted by user")
        checkpoint.save()
        wiki_cache.save()
        logger.info("💾 Progress saved. Resume anytime.")
        sys.exit(1)

    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
        checkpoint.save()
        wiki_cache.save()
        sys.exit(1)


if __name__ == "__main__":
    main()