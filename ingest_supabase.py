import asyncio
import asyncpg
import pandas as pd
import json
import os

from datetime import datetime
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================
CSV_PATH = "final_data/calyx_species_data.csv"

load_dotenv()
print("ENV:", os.getenv("SUPABASE_DB_URL"))

DATABASE_URL = os.getenv("SUPABASE_DB_URL")
print("DB URL:", DATABASE_URL)

CHUNK_SIZE = 10000
BATCH_SIZE = 2000
MAX_RETRIES = 3

SEM = asyncio.Semaphore(2)  # Limit concurrent transactions

INSERT_SQL = """
INSERT INTO species (
    scientific_name,
    common_names,
    family,
    description,
    primary_image_url,
    order_name,
    genus,
    species,
    gbif_id,
    inat_id,
    observation_count,
    inat_status,
    has_images,
    description_source
)
VALUES (
    $1,$2,$3,$4,$5,
    $6,$7,$8,$9,$10,
    $11,$12,$13,$14
)
ON CONFLICT (scientific_name)
DO UPDATE SET
    common_names = EXCLUDED.common_names,
    family = EXCLUDED.family,
    description = EXCLUDED.description,
    primary_image_url = EXCLUDED.primary_image_url,
    order_name = EXCLUDED.order_name,
    genus = EXCLUDED.genus,
    species = EXCLUDED.species,
    gbif_id = EXCLUDED.gbif_id,
    inat_id = EXCLUDED.inat_id,
    observation_count = EXCLUDED.observation_count,
    inat_status = EXCLUDED.inat_status,
    has_images = EXCLUDED.has_images,
    description_source = EXCLUDED.description_source
"""

# =========================
# HELPERS
# =========================

def safe_json_load(val):
    if pd.isna(val):
        return []

    try:
        return json.loads(val)
    except json.JSONDecodeError:
        print(f"Bad JSON: {val[:100]}")
        return []

def clean_text(value):
    if value is None:
        return None

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    if value.lower() == "nan":
        return None

    return value

def pick_primary_image(inat_images, wiki_images):
    if inat_images:
        return inat_images[0]
    if wiki_images:
        return wiki_images[0]
    return None

def merge_species_rows(rows):
    first = rows[0]

    common_names = sorted({
        str(r["common_name"]).strip()
        for r in rows
        if pd.notna(r["common_name"])
        and str(r["common_name"]).strip()
    })

    descriptions = [
        r["wikipedia_description"]
        for r in rows
        if pd.notna(r["wikipedia_description"])
        and str(r["wikipedia_description"]).strip()
    ]

    description = max(
        descriptions,
        key=len,
        default=None
    )

    inat_images = []
    wiki_images = []

    for r in rows:
        inat_images.extend(
            safe_json_load(r["inat_research_images"])
        )

        wiki_images.extend(
            safe_json_load(r["wiki_images"])
        )

    inat_images = list(dict.fromkeys(inat_images))
    wiki_images = list(dict.fromkeys(wiki_images))

    merged = first.copy()

    merged["common_names_array"] = common_names
    merged["wikipedia_description"] = description
    merged["inat_research_images"] = json.dumps(inat_images)
    merged["wiki_images"] = json.dumps(wiki_images)

    return merged

def is_valid_species_name(name):
    if pd.isna(name):
        return False

    name = str(name).strip().lower()
    
    

    if name == "x":
        return False

    if " x " in name:
        return False

    return True

# =========================
# CORE INGEST LOGIC
# =========================

async def insert_batch(pool, batch):
    for attempt in range(MAX_RETRIES):
        try:
            async with SEM:
                async with pool.acquire() as conn:
                    async with conn.transaction():

                        species_rows = []
                        image_rows = []

                        for row in batch:
                            if pd.isna(row["family"]):
                                print("Missing family:", row["scientific_name"])

                            if pd.isna(row["species"]):
                                print("Missing species:", row["scientific_name"])
                            
                            if not is_valid_species_name(row["scientific_name"]):
                                continue

                            inat_images = safe_json_load(row["inat_research_images"])
                            wiki_images = safe_json_load(row["wiki_images"])

                            primary = pick_primary_image(
                                inat_images,
                                wiki_images
                            )
                            
                            description_source = (
                                str(clean_text(row["description_source"])).strip().lower()
                                if pd.notna(clean_text(row["description_source"]))
                                else None
                            )

                            species_rows.append(
                            (
                                clean_text(row["scientific_name"]),
                                row.get("common_names_array", []),
                                clean_text(row["family"]),
                                clean_text(row["wikipedia_description"]),
                                primary,

                                clean_text(row["order"]),
                                clean_text(row["genus"]),

                                clean_text(row["species"]),

                                int(row["gbif_id"])
                                if pd.notna(row["gbif_id"])
                                else None,

                                int(row["inat_id"])
                                if pd.notna(row["inat_id"])
                                else None,

                                int(row["observation_count"])
                                if pd.notna(row["observation_count"])
                                else None,

                                clean_text(row["inat_status"]),

                                str(row["has_images"]).lower() == "true",

                                description_source
                            ))

                        await conn.executemany(
                            INSERT_SQL,
                            species_rows
                        )

                        print(
                            f"Inserted/updated {len(species_rows)} species"
                        )

                        species_ids = await conn.fetch("""
                            SELECT id, scientific_name
                            FROM species
                            WHERE scientific_name = ANY($1::text[])
                        """,
                        [r[0] for r in species_rows]
                        )

                        id_map = {r["scientific_name"]: r["id"] for r in species_ids}

                        # =========================
                        # IMAGES
                        # =========================
                        for row in batch:

                            seen_images = set()

                            sid = id_map.get(row["scientific_name"])
                            if not sid:
                                continue

                            inat_images = safe_json_load(row["inat_research_images"])
                            wiki_images = safe_json_load(row["wiki_images"])
                            
                            order = 0

                            for i, url in enumerate(inat_images):
                                if url in seen_images:
                                    continue

                                seen_images.add(url)

                                image_rows.append(
                                    (
                                        sid,
                                        url,
                                        "inat",
                                        i == 0,
                                        order
                                    )
                                )

                                order += 1

                            for url in wiki_images:
                                if url in seen_images:
                                    continue

                                seen_images.add(url)

                                image_rows.append(
                                    (
                                        sid,
                                        url,
                                        "wiki",
                                        False,
                                        order
                                    )
                                )

                                order += 1

                        if image_rows:
                            await conn.executemany("""
                                INSERT INTO species_images (
                                    species_id,
                                    image_url,
                                    source,
                                    is_primary,
                                    image_order
                                )
                                VALUES ($1,$2,$3,$4,$5)
                                ON CONFLICT DO NOTHING;
                            """, image_rows)

                    return len(image_rows)

        except Exception as e:
            print(f"⚠️ Retry {attempt+1}: {e}")
            await asyncio.sleep(2 ** attempt)

    return None

# =========================
# MAIN RUNNER
# =========================

async def run():
    start_time = datetime.now()

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"CSV not found: {CSV_PATH}"
        )

    print("🚀 Starting ingestion...")

    print("📄 Scanning CSV...")

    csv_rows = sum(
        1 for _ in open(CSV_PATH, encoding="utf-8")
    ) - 1

    print(f"📊 Rows detected: {csv_rows:,}")

    if not DATABASE_URL:
        raise ValueError(
            "SUPABASE_DB_URL not found in environment"
        )
    
    print(DATABASE_URL[:40] + "...")


    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        statement_cache_size=0
    )
    

    print("\n===== CONNECTION DEBUG =====")
    
    async with pool.acquire() as conn:
        project = await conn.fetchval(
            "SELECT current_database();"
        )
    print("Database:", project)

    print("DATABASE_URL:")
    print(DATABASE_URL)

    print("\n🔌 Testing database connection...")
    print("============================\n")

    try:
        async with pool.acquire() as conn:
            version = await conn.fetchval(
                "SELECT version();"
            )

            species_count = await conn.fetchval(
                "SELECT COUNT(*) FROM species"
            )
            
            image_count = await conn.fetchval(
                "SELECT COUNT(*) FROM species_images"
            )
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM species"
            )

        print("✅ Connected to Supabase")
        print(f"🐘 PostgreSQL: {version.split(',')[0]}")

        print("\n📋 Existing rows before ingest:")
        print(f"   Species: {species_count:,}")
        print(f"   Images : {image_count:,}")


    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    total_input_rows = 0
    total_unique_species = 0

    success_rows = 0
    total_images = 0

    for chunk_idx, df in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
        print(df.columns.tolist())

        print(f"\n📦 Chunk {chunk_idx+1} loaded ({len(df)} rows)")
        
        total_input_rows += len(df)

        records = []

        for _, group in df.groupby("scientific_name"):
            merged = merge_species_rows(
                group.to_dict("records")
            )

            records.append(merged)

        total_unique_species += len(records)

        tasks = []

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]

            tasks.append(
                (
                    batch,
                    insert_batch(pool, batch)
                )
            )

        results = await asyncio.gather(
            *(task[1] for task in tasks),
            return_exceptions=True
        )

        for (batch, _), result in zip(tasks, results):

            if isinstance(result, Exception):
                continue

            if isinstance(result, int):
                success_rows += len(batch)
                total_images += result

        pct = total_input_rows / csv_rows * 100

        async with pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM species
            """)

        print(f"Processed {success_rows} species so far")
        print(f"DEBUG species count: {count}")

        print(
            f"\n📊 Overall Progress: "
            f"{total_input_rows:,}/{csv_rows:,} "
            f"({pct:.2f}%)"
        )
    
    async with pool.acquire() as conn:
        final_species = await conn.fetchval(
            "SELECT COUNT(*) FROM species"
        )

        final_images = await conn.fetchval(
            "SELECT COUNT(*) FROM species_images"
        )

    elapsed = datetime.now() - start_time

    await pool.close()

    print("\n" + "="*60)
    print("🎉 INGESTION COMPLETE")
    print("="*60)
    
    print(
        f"{total_input_rows:,} CSV rows -> "
        f"{total_unique_species:,} unique species"
    )

    print(f"🌿 Species processed : {success_rows:,}")
    print(f"🖼️ Images processed  : {total_images:,}")

    print("\n📋 Database Totals")
    print(f"Species table : {final_species:,}")
    print(f"Images table  : {final_images:,}")

    print(f"⏱ Runtime: {elapsed}")

# =========================
# ENTRY
# =========================

if __name__ == "__main__":
    asyncio.run(run())