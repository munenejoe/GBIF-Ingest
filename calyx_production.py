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
import csv
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
MAX_CONCURRENT_WIKI = 10     # Concurrent Wikipedia requests
GBIF_BATCH_SIZE = 100        # Species per GBIF request
CHECKPOINT_INTERVAL = 500    # Save checkpoint every N species
REQUEST_TIMEOUT = 15         # Seconds
MAX_RETRIES = 3              # Retry failed requests
GLOBAL_MAX = 300000         # Global CAP on total species to prevent runaway

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
    observation_count: Optional[int]
    wikipedia_description: str
    description_source: str  # "species", "cleaned", "genus", "none"
    extraction_timestamp: str
    image_urls: str # pipe-separated or JSON string of image URLs

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

    if logger.handlers:  # ✅ prevent duplicate logs
        return logger
    
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
        if len(self.processed_ids) > 500000:
            self.processed_ids = set(list(self.processed_ids)[:-200000])

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

class DeadLetterQueue:
    def __init__(self, file="dead_letter.jsonl"):
        self.file = Path(file)

    def push(self, payload: Dict):
        try:
            with open(self.file, "a") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception as e:
            logger.error(f"[DLQ WRITE ERROR] {e}")

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

async def fetch_gbif_species(
    session: aiohttp.ClientSession,
    order_name: str,
    order_key: int,
    limit: int,
    checkpoint: CheckpointManager
) -> List[Dict]:
    """
    Asynchronously fetch all species for a given order.
    Uses pagination with parallel requests.
    """
    logger.info(f"🌸 Fetching species for {order_name} (Key: {order_key})")
    
    all_species = []
    batch_size = GBIF_BATCH_SIZE
    
    # Skip already processed species
    already_processed = checkpoint.get_order_count(order_name)
    if already_processed > 0:
        logger.info(f"  ↻ Resuming from {already_processed} processed species")
        offset = already_processed
    
    while len(all_species) < limit:
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
                    logger.warning(f"  ⚠️  GBIF returned status {response.status} at offset {offset}")
                    break
                
                data = await response.json()
                results = data.get("results", [])
                
                if not results:
                    logger.info(f"  ✓ No more results at offset {offset}")
                    break
                
                # Filter out already processed
                new_results = [
                    r for r in results 
                    if not checkpoint.is_processed(r.get("key"))
                ]
                
                all_species.extend(new_results)
                offset += batch_size
                
                logger.info(f"  → Fetched {len(new_results)} new species (offset: {offset})")
                
                if len(all_species) >= limit:
                    break
                
                await asyncio.sleep(0.2)  # Rate limiting
                
        except asyncio.TimeoutError:
            logger.warning(f"  ⏱️  Timeout at offset {offset}")
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
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
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

# ============================================================================
# ASYNC INATURALIST FETCHER
# ============================================================================

INAT_CONCURRENCY = 5
inat_semaphore = asyncio.Semaphore(INAT_CONCURRENCY)

async def fetch_inat_data(session: aiohttp.ClientSession, scientific_name: str) -> Dict:
    """
    Fetch iNaturalist data with:
    - Concurrency control (semaphore)
    - Strong validation
    - Image prioritisation
    - Graceful fallback
    """

    async with inat_semaphore:  # 🔥 CRITICAL FIX
        try:
            # ---------------------------
            # STEP 1: Resolve taxon
            # ---------------------------
            taxon_params = {
                "q": scientific_name,
                "rank": "species",
                "per_page": 1
            }

            async with session.get(INAT_API_URL, params=taxon_params) as response:
                if response.status != 200:
                    return fallback_inat()

                data = await response.json()
                results = data.get("results", [])

                if not results:
                    return fallback_inat()

                taxon = results[0]
                taxon_id = taxon.get("id")

                if not taxon_id:
                    return fallback_inat()

            # Small delay to avoid burst patterns
            await asyncio.sleep(0.08)

            # ---------------------------
            # STEP 2: Fetch observations (BEST images)
            # ---------------------------
            obs_url = "https://api.inaturalist.org/v1/observations"
            obs_params = {
                "taxon_id": taxon_id,
                "photos": "true",
                "per_page": 12
            }

            image_urls = []

            async with session.get(obs_url, params=obs_params) as obs_response:
                if obs_response.status == 200:
                    obs_data = await obs_response.json()
                    observations = obs_data.get("results", [])

                    for obs in observations:
                        for photo in obs.get("photos", []):
                            url = photo.get("url")

                            if not url:
                                continue

                            # upgrade resolution
                            url = url.replace("square", "medium")

                            if url not in image_urls:
                                image_urls.append(url)

                            if len(image_urls) >= 5:
                                break

                        if len(image_urls) >= 5:
                            break

            # ---------------------------
            # STEP 3: Taxon fallback images
            # ---------------------------
            if not image_urls:
                for photo in taxon.get("taxon_photos", []):
                    url = photo.get("photo", {}).get("medium_url")

                    if url and url not in image_urls:
                        image_urls.append(url)

                    if len(image_urls) >= 3:
                        break

            # ---------------------------
            # STEP 4: Final payload
            # ---------------------------
            return {
                "inat_id": taxon_id,
                "observations": taxon.get("observations_count"),
                "images": image_urls[:5],
                "common_name": (
                    taxon.get("preferred_common_name")
                    or taxon.get("english_common_name")
                    or ""
                )
            }

        except asyncio.TimeoutError:
            logger.warning(f"[INAT TIMEOUT] {scientific_name}")
        except Exception as e:
            logger.error(f"[INAT ERROR] {scientific_name}: {e}")

    return fallback_inat()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fallback_inat():
    return {
        "inat_id": None,
        "observations": None,
        "images": [],
        "common_name": ""
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
# ASYNC WIKIPEDIA FETCHER WITH STRICT CHAIN
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

async def fetch_wikimedia_images(session, name: str) -> List[str]:
    """Fetch images strictly from a specific Wikimedia page."""

    base_url = "https://commons.wikimedia.org/w/api.php"

    params = {
        "action": "query",
        "titles": name,
        "prop": "images",
        "imlimit": 10,
        "format": "json"
    }

    try:
        async with session.get(base_url, params=params) as response:
            if response.status != 200:
                return []

            data = await response.json()
            pages = data.get("query", {}).get("pages", {})

            image_titles = []
            for page in pages.values():
                if "images" not in page:
                    return []  # 🚨 no images → treat as fail
                image_titles = [img["title"] for img in page["images"]]

        # STEP 2: Resolve image titles → URLs
        image_urls = []

        for title in image_titles[:10]:
            img_params = {
                "action": "query",
                "titles": title,
                "prop": "imageinfo",
                "iiprop": "url|extmetadata",
                "format": "json"
            }

            async with session.get(base_url, params=img_params) as img_res:
                if img_res.status != 200:
                    continue

                img_data = await img_res.json()
                img_pages = img_data.get("query", {}).get("pages", {})

                for img_page in img_pages.values():
                    info = img_page.get("imageinfo", [])
                    if info:
                        url = info[0].get("url")

                        metadata = info[0].get("extmetadata", {})

                        description = metadata.get("ImageDescription", {}).get("value", "").lower()
                        categories = metadata.get("Categories", {}).get("value", "").lower()

                        width = int(metadata.get("ImageWidth", {}).get("value", 0))
                        height = int(metadata.get("ImageHeight", {}).get("value", 0))

                        if width < 800 or height < 600:
                            continue  # skip low-res junk

                        # 🚫 Reject junk / non-photographic
                        bad_keywords = [
                            "diagram", "drawing", "illustration", "herbarium",
                            "map", "distribution", "chart", "graph",
                            "logo", "icon", "seal"
                        ]

                        if any(k in description for k in bad_keywords):
                            continue

                        if any(k in categories for k in bad_keywords):
                            continue

                        # ✅ Enforce species-level match
                        binomial = extract_binomial(name)
                        if binomial:
                            genus, species = binomial.lower().split()

                            if genus not in description and species not in description:
                                continue

                        # ✅ Accept only real photos
                        if url and url.lower().endswith((".jpg", ".jpeg", ".png")):
                            image_urls.append(url)

                if len(image_urls) >= 5:
                    break

        return image_urls

    except Exception as e:
        print(f"[WIKI IMG ERROR] {name}: {e}")
        return []
    
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

async def fetch_wiki_images_strict(session, species_name: str) -> List[str]:
    """
    ONLY fetch images from species page.
    NO genus fallback.
    Skip if not clean + relevant.
    """

    base_url = "https://commons.wikimedia.org/w/api.php"

    params = {
        "action": "query",
        "titles": species_name,
        "prop": "images",
        "imlimit": 10,
        "format": "json"
    }

    try:
        async with session.get(base_url, params=params) as response:
            if response.status != 200:
                return []

            data = await response.json()
            pages = data.get("query", {}).get("pages", {})

            image_titles = []
            for page in pages.values():
                if "images" not in page:
                    return []  # 🚫 no species images → skip
                image_titles = [img["title"] for img in page["images"]]

        image_urls = []

        for title in image_titles[:10]:
            img_params = {
                "action": "query",
                "titles": title,
                "prop": "imageinfo",
                "iiprop": "url|extmetadata",
                "format": "json"
            }

            async with session.get(base_url, params=img_params) as img_res:
                if img_res.status != 200:
                    continue

                img_data = await img_res.json()
                pages = img_data.get("query", {}).get("pages", {})

                for img_page in pages.values():
                    info = img_page.get("imageinfo", [])
                    if not info:
                        continue

                    meta = info[0].get("extmetadata", {})
                    url = info[0].get("url")

                    if not url:
                        continue

                    description = meta.get("ImageDescription", {}).get("value", "").lower()
                    categories = meta.get("Categories", {}).get("value", "").lower()

                    # 🚫 junk filters
                    bad = ["diagram", "drawing", "illustration", "map", "logo", "icon", "chart"]
                    if any(k in description for k in bad):
                        continue
                    if any(k in categories for k in bad):
                        continue

                    # ✅ enforce species match
                    binomial = extract_binomial(species_name)
                    if binomial:
                        g, s = binomial.lower().split()
                        if g not in description and s not in description:
                            continue

                    # ✅ resolution filter
                    width = int(meta.get("ImageWidth", {}).get("value", 0))
                    height = int(meta.get("ImageHeight", {}).get("value", 0))

                    if width < 800 or height < 600:
                        continue

                    if url.lower().endswith((".jpg", ".jpeg", ".png")):
                        image_urls.append(url)

                if len(image_urls) >= 5:
                    break

        return image_urls

    except Exception as e:
        print(f"[WIKI IMG ERROR] {species_name}: {e}")
        return []

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

    if attempt == retries - 1:
        logger.warning(f"  ❌ All retries failed for {func.__name__} with args {args} kwargs {kwargs}")
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

def final_dataset_check(csv_path: str):
    df = pd.read_csv(csv_path)

    total = len(df)

    missing_desc = (df["wikipedia_description"] == "Description not found.").sum()
    missing_images = df["image_urls"].apply(lambda x: len(json.loads(x)) == 0).sum()
    missing_common = (df["common_name"] == "").sum()

    logger.info("\n📊 FINAL DATA QUALITY REPORT")
    logger.info(f"Total records: {total}")
    logger.info(f"No description: {missing_desc} ({missing_desc/total:.2%})")
    logger.info(f"No images: {missing_images} ({missing_images/total:.2%})")
    logger.info(f"No common name: {missing_common} ({missing_common/total:.2%})")

# ============================================================================
# SPECIES PROCESSOR
# ============================================================================

async def process_species(
    species_data: Dict,
    order_name: str,
    session: aiohttp.ClientSession,
    wiki_cache: WikipediaCache
) -> SpeciesRecord:
    """
    Process a single species record.
    Fully defensive against None returns from async retries.
    """

    canonical_name = species_data.get("canonicalName") or ""
    genus = species_data.get("genus") or ""

    inat_schema = Schema({
        "inat_id": SchemaField((int, type(None)), default=None),
        "observations": SchemaField((int, type(None)), default=None),
        "images": SchemaField(list, default=[], transform=lambda x: x[:5]),
        "common_name": SchemaField(str, default="", transform=lambda x: x.strip())
    })

    wiki_schema = Schema({
        "description": SchemaField(str, default="Description not found."),
        "source": SchemaField(str, default="none")
    })

    image_list_schema = Schema({
        "images": SchemaField(list, default=[])
    })


    async def enrich_species_with_retry():
        description, source = "Description not found.", "none"
        inat_data = {}
        wiki_images = []

        for attempt in range(3):
            wiki_task = retry_async(
                fetch_wikipedia_description,
                session, canonical_name, genus, wiki_cache,
                expected_type=tuple,
                validator=lambda r: r and len(r[0]) > 50
            )

            inat_task = retry_async(
                fetch_inat_data,
                session,
                canonical_name,
                expected_type=dict,
                validator=lambda r: r and (
                    r.get("images") or
                    r.get("common_name") or
                    r.get("observations")
                )
            )

            wiki_img_task = retry_async(
                fetch_wiki_images_strict,
                session,
                canonical_name,
                expected_type=list,
                validator=lambda r: isinstance(r, list) and len(r) > 0
            )

            desc_res, inat_res, img_res = await asyncio.gather(
                wiki_task,
                inat_task,
                wiki_img_task
            )

            # ✅ HARD TYPE GUARDS (CRITICAL)
            if isinstance(desc_res, tuple):
                description, source = desc_res

            wiki_data_raw = normalize_wiki_result(desc_res)
            inat_data_raw = inat_res if isinstance(inat_res, dict) else {}
            wiki_images_raw = {"images": img_res} if isinstance(img_res, list) else {}

            wiki_data = wiki_schema.validate(wiki_data_raw)
            inat_data = inat_schema.validate(inat_data_raw)
            wiki_images = wiki_images_raw["images"]

            if is_record_complete(inat_data, description, wiki_images):
                return description, source, inat_data, wiki_images

            await asyncio.sleep(0.5 * (attempt + 1))

        return description, source, inat_data, wiki_images

    # 🔍 Extract species safely
    name_parts = canonical_name.split()
    species = name_parts[1] if len(name_parts) > 1 else ""

    # 🔥 Fetch enriched data
    description, source, inat_data, wiki_images = await enrich_species_with_retry()

    # ✅ EXTRA DEFENSIVE GUARDS (belt + suspenders)
    inat_data = inat_data or {}
    wiki_images = wiki_images or []

    merged_images = merge_images(
        inat_data.get("images", []),
        wiki_images
    )

    record = SpeciesRecord(
        order=order_name,
        family=species_data.get("family") or "",
        genus=genus,
        species=species,
        scientific_name=species_data.get("scientificName") or "",
        common_name=clean_common_name(
            inat_data["common_name"] or species_data.get("vernacularName") or ""
        ),
        gbif_id=species_data.get("key") or 0,
        inat_id=inat_data["inat_id"],
        observation_count=inat_data["observations"],
        wikipedia_description=description,
        description_source=source,
        image_urls=json.dumps(merged_images),
        extraction_timestamp=datetime.now().isoformat()
    )

    final_record_schema = Schema({
        "scientific_name": SchemaField(str, required=True),
        "gbif_id": SchemaField(int, required=True),
        "order": SchemaField(str, required=True),
        "genus": SchemaField(str, default=""),
        "species": SchemaField(str, default=""),
        "common_name": SchemaField(str, default=""),
        "image_urls": SchemaField(str, default="[]"),
        "wikipedia_description": SchemaField(str, default="Description not found.")
    })

    raw_record = record.to_dict()
    validated = final_record_schema.validate(raw_record)

    # HARD FAIL if critical fields missing
    if not validated["scientific_name"] or not validated["gbif_id"]:
        raise ValueError("Critical record validation failed")

    return record


# ============================================================================
# CSV WRITER (STREAMING)
# ============================================================================

class StreamingCSVWriter:
    def __init__(self, output_file: str):
        self.output_file = Path(output_file)
        self.file = open(self.output_file, "a", newline="", encoding="utf-8")
        self.writer = None

    def write_batch(self, records: List[SpeciesRecord]):
        if not records:
            return

        rows = [r.to_dict() for r in records]

        if not self.writer:
            self.writer = csv.DictWriter(self.file, fieldnames=rows[0].keys())
            if self.file.tell() == 0:
                self.writer.writeheader()

        self.writer.writerows(rows)
        self.file.flush()
    
    def close(self):
        if self.file:
            self.file.close()

# ============================================================================
# MAIN EXTRACTION ORCHESTRATOR
# ============================================================================

dlq = DeadLetterQueue()

async def worker(name, queue, results, session, wiki_cache, order_name):
    while True:
        species_data = await queue.get()

        if species_data is None:
            queue.task_done()
            break

        try:
            record = await process_species(
                species_data, order_name, session, wiki_cache
            )

            if isinstance(record, SpeciesRecord):
                await results.put(record)
            else:
                raise ValueError("Invalid record type")

        except Exception as e:
            dlq.push({
                "error": str(e),
                "species": species_data,
                "timestamp": datetime.now().isoformat()
            })

        finally:
            queue.task_done()

async def extract_order_parallel(
    order_name: str,
    order_key: int,
    limit_per_order: int,
    checkpoint: CheckpointManager,
    wiki_cache: WikipediaCache,
    csv_writer: StreamingCSVWriter,
    session: aiohttp.ClientSession
):
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Order: {order_name}")
    logger.info(f"{'='*60}")

    species_list = await fetch_gbif_species(
        session, order_name, order_key, limit_per_order, checkpoint
    )

    if not species_list:
        logger.warning(f"⚠️ No species found for {order_name}")
        return

    logger.info(f"🔄 Processing {len(species_list)} species...")

    queue = asyncio.Queue()
    results_queue = asyncio.Queue(maxsize=1000)

    for sp in species_list:
        await queue.put(sp)

    # ✅ FIXED worker pool
    workers = [
        asyncio.create_task(
            worker(i, queue, results_queue, session, wiki_cache, order_name)
        )
        for i in range(MAX_CONCURRENT_WIKI)
    ]

    batch = []
    processed_count = 0

    async def result_collector():
        nonlocal batch, processed_count

        while True:
            record = await results_queue.get()

            if record is None:
                break

            batch.append(record)
            processed_count += 1

            # ✅ STREAMING WRITE (CRITICAL FOR AWS)
            if len(batch) >= CHECKPOINT_INTERVAL:
                csv_writer.write_batch(batch)

                for r in batch:
                    checkpoint.mark_processed(r.gbif_id, order_name)

                checkpoint.save()
                wiki_cache.save()

                logger.info(f"💾 Checkpoint saved ({processed_count})")

                batch.clear()

            results_queue.task_done()

            if len(checkpoint.processed_ids) >= GLOBAL_MAX:
                logger.warning("🚨 Global max processed records reached. Stopping.")
                return

    collector_task = asyncio.create_task(result_collector())

    # wait for workers to finish processing queue
    await queue.join()

    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, return_exceptions=True)

    # signal collector to stop
    await results_queue.put(None)
    await collector_task

    # ✅ FINAL FLUSH
    if batch:
        csv_writer.write_batch(batch)

        for r in batch:
            checkpoint.mark_processed(r.gbif_id, order_name)

        batch.clear()

    checkpoint.save()
    if processed_count % 2000 == 0:
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
    if batch_num not in BATCH_ORDER_NAMES:
        logger.error(f"❌ Invalid batch number: {batch_num}")
        return

    order_names = BATCH_ORDER_NAMES[batch_num]

    logger.info(f"\n{'='*80}")
    logger.info(f"🌿 BATCH {batch_num}: {len(order_names)} orders")
    logger.info(f"{'='*80}\n")

    order_keys = sync_backbone_keys(order_names)

    if not order_keys:
        logger.error("❌ No order keys resolved.")
        return

    # ✅ ONE SESSION FOR ENTIRE BATCH
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for order_name, order_key in order_keys.items():
            await extract_order_parallel(
                order_name,
                order_key,
                limit_per_order,
                checkpoint,
                wiki_cache,
                csv_writer,
                session   # 🔥 pass session
            )

            await asyncio.sleep(1.5)

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Calyx Production Data Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract single batch
  python calyx_production.py --batch 1 --limit 5000
  
  # Extract specific orders
  python calyx_production.py --orders Asterales Rosales --limit 10000
  
  # Extract all batches
  python calyx_production.py --all --limit 50000
  
  # Resume from checkpoint
  python calyx_production.py --batch 1 --limit 5000 --resume
        """
    )
    
    parser.add_argument(
        '--batch',
        type=int,
        choices=range(1, 10),
        help='Batch number to extract (1-9)'
    )
    parser.add_argument(
        '--orders',
        nargs='+',
        help='Specific order names to extract'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Extract all batches sequentially'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10000,
        help='Max species per order (default: 10000)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='calyx_species_data.csv',
        help='Output CSV filename'
    )
    parser.add_argument(
        '--checkpoint',
        type=str,
        default='calyx_checkpoint.json',
        help='Checkpoint file for resume capability'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Optional log file path'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from checkpoint (auto-detected if checkpoint exists)'
    )
    
    args = parser.parse_args()
    
    # Validation
    if not any([args.batch, args.orders, args.all]):
        parser.print_help()
        print("\n❌ Error: Specify --batch, --orders, or --all")
        sys.exit(1)
    
    # Setup logging
    global logger
    logger = setup_logging(args.log_file)
    
    # Initialize components
    checkpoint = CheckpointManager(args.checkpoint)
    wiki_cache = WikipediaCache()
    csv_writer = StreamingCSVWriter(args.output)
    
    logger.info("🌺 CALYX PRODUCTION DATA EXTRACTION PIPELINE 🌺")
    logger.info(f"Output: {args.output}")
    logger.info(f"Checkpoint: {args.checkpoint}")
    logger.info(f"Limit per order: {args.limit:,}")
    logger.info(f"Max concurrent Wikipedia: {MAX_CONCURRENT_WIKI}")
    
    start_time = time.time()
    
    # Determine what to extract
    try:
        if args.all:
            # Extract all batches
            for batch_num in range(1, 10):
                asyncio.run(extract_batch(
                    batch_num, args.limit, checkpoint, wiki_cache, csv_writer
                ))
        
        elif args.batch:
            # Extract single batch
            asyncio.run(extract_batch(
                args.batch, args.limit, checkpoint, wiki_cache, csv_writer
            ))
        
        elif args.orders:
            # Extract specific orders
            order_keys = sync_backbone_keys(args.orders)
            
            async def extract_custom_orders():
                connector = aiohttp.TCPConnector(limit=100)
                timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    for order_name, order_key in order_keys.items():
                        await extract_order_parallel(
                            order_name,
                            order_key,
                            args.limit,
                            checkpoint,
                            wiki_cache,
                            csv_writer,
                            session   # 🔥 pass session
                        )
            
            asyncio.run(extract_custom_orders())
        
        # Final saves
        checkpoint.save()
        wiki_cache.save()
        
        # Summary
        elapsed = (time.time() - start_time) / 60
        logger.info(f"\n{'='*80}")
        logger.info("✨ EXTRACTION COMPLETE!")
        logger.info(f"{'='*80}")
        logger.info(f"📁 Output: {args.output}")
        logger.info(f"⏱️  Time: {elapsed:.2f} minutes")
        logger.info(f"✅ Success!")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted by user")
        checkpoint.save()
        wiki_cache.save()
        logger.info("💾 Progress saved. Resume with --resume flag")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
        checkpoint.save()
        wiki_cache.save()
        sys.exit(1)
    finally:
        csv_writer.close()

if __name__ == "__main__":
    main()