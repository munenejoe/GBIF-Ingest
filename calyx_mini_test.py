"""
CALYX MINI DATA EXTRACTION TEST
================================
This script performs a small-scale test of the GBIF + Wikipedia data pipeline
to verify data quality before running the full extraction on Oracle server.

Features:
- Fetches a small sample from each order
- Validates that we're getting flowering plants (Angiosperms)
- Checks data completeness
- Shows preview of extracted data
"""

import requests
import pandas as pd
import time
from collections import Counter

# --- CONFIGURATION ---
GBIF_SPECIES_URL = "https://api.gbif.org/v1/species/search"
GBIF_TAXON_URL = "https://api.gbif.org/v1/species/"
WIKI_API_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/"
INATURALIST_API_URL = "https://api.inaturalist.org/v1/taxa"

# Updated with all your batch orders
ORDERS_NAMES = {
    # Batch 1: The Monocots (A)
    "Asparagales",
    "Dipsacales",
    "Liliales",
    
    # Batch 2: The Monocots (B)
    "Poales",
    "Arecales",
    "Alismatales",
    
    # Batch 3: The Asterids (A)
    "Asterales",
    
    # Batch 4: The Asterids (B)
    "Lamiales",
    "Gentianales",
    
    # Batch 5: The Asterids (C)
    "Solanales",
    "Ericales",
    "Apiales",
    
    # Batch 6: The Rosids (A)
    "Rosales",
    "Fabales",
    
    # Batch 7: The Rosids (B)
    "Malpighiales",
    "Myrtales",
    
    # Batch 8: The Rosids (C)
    "Brassicales",
    "Sapindales",
    "Malvales",
    
    # Batch 9: Basal & Others
    "Magnoliales",
    "Ranunculales",
    "Caryophyllales",
}


def verify_order_is_angiosperm(order_key):
    # Use the species/match or ensure you're looking at the Backbone specifically
    # DatasetKey d7dddbf4-2cf0-4f39-9b2a-bb099caae36c is the GBIF Backbone
    url = f"https://api.gbif.org/v1/species/{order_key}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            # Double check that we are in the Plantae kingdom
            if data.get("kingdom") != "Plantae":
                return None 
            
            kingdom = data.get("kingdom", "")
            phylum = data.get("phylum", "")
            class_name = data.get("class", "")
            
            is_angiosperm = (
                kingdom == "Plantae" and 
                class_name in ["Magnoliopsida", "Liliopsida"]
            )
            return {
                "is_angiosperm": is_angiosperm,
                "kingdom": kingdom,
                "phylum": phylum,
                "class": class_name,
                "order": data.get("order", "")
            }

    except Exception:
        pass
    return None

def fetch_wiki_description(name):
    """Fetch a clean Wikipedia summary with smart fallback + retry logic."""

    headers = {
        "User-Agent": "CalyxBot/1.0 (joe.munene@email.com)"
    }

    def fetch(title):
        try:
            url = WIKI_API_URL + title.replace(" ", "_")
            res = requests.get(url, headers=headers, timeout=5)

            if res.status_code == 200:
                data = res.json()
                return data.get("extract", "").strip()
        except:
            pass
        return None

    try:
        # --- 1. exact species ---
        desc = fetch(name)

        # --- 2. cleaned name (remove author/year noise if any) ---
        if not desc:
            cleaned = " ".join(name.split()[:2])  # genus + species only
            if cleaned != name:
                desc = fetch(cleaned)

        # --- 3. genus fallback ---
        if not desc:
            genus = name.split(" ")[0]
            desc = fetch(genus)

        # --- final output ---
        if desc:
            return desc

        print(f"⚠️ Wiki missing: {name}")
        return ""

    except Exception as e:
        print(f"❌ Wiki error: {name} | {e}")
        return desc, "species" # "genus" / "missing"

def fetch_inat_images(name, max_images=3):
    """Fetch multiple high-quality images + metadata from iNaturalist."""

    def extract_taxon_data(taxon):
        images = []

        for photo in taxon.get("taxon_photos", []):
            photo_data = photo.get("photo", {})
            url = (
                photo_data.get("large_url")
                or photo_data.get("medium_url")
                or photo_data.get("square_url")
            )
            if url:
                images.append(url)

            if len(images) >= max_images:
                break

        return {
            "inat_id": taxon.get("id"),
            "common_name": taxon.get("preferred_common_name"),
            "observations_count": taxon.get("observations_count"),
            "image_primary": images[0] if images else "",
            "image_gallery": images
        }

    params = {
        "q": name,
        "rank": "species",
        "per_page": 1,
        "order": "desc",
        "order_by": "observations_count"
    }

    try:
        # --- PRIMARY: species match ---
        res = get_with_retry(INATURALIST_API_URL, params)

        if res and res.get("results"):
            return extract_taxon_data(res["results"][0])

        # --- FALLBACK: genus match ---
        genus = name.split(" ")[0]
        params["q"] = genus

        res = get_with_retry(INATURALIST_API_URL, params)

        if res and res.get("results"):
            return extract_taxon_data(res["results"][0])

    except Exception:
        pass

    # --- DEFAULT ---
    return {
        "inat_id": "",
        "common_name": "",
        "observations_count": 0,
        "image_primary": "",
        "image_gallery": []
    }

def test_order_extraction(order_name, order_key, test_limit=5):
    """
    Tests extraction from a single order and returns sample data + stats.
    """
    print(f"\n🔍 Testing: {order_name} (Key: {order_key})")
    
    # First verify this is an angiosperm order
    verification = verify_order_is_angiosperm(order_key)
    if verification:
        print(f"  Kingdom: {verification['kingdom']}")
        print(f"  Phylum: {verification['phylum']}")
        print(f"  Class: {verification['class']}")
        print(f"  ✓ Is Angiosperm: {verification['is_angiosperm']}")
        
        if not verification['is_angiosperm']:
            print(f"  ⚠️  WARNING: This may not be a flowering plant order!")
    
    params = {
        "highertaxonKey": order_key,
        "datasetKey": "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c",
        "rank": "SPECIES",
        "status": "ACCEPTED",
        "limit": test_limit,
        "offset": 0
    }
    
    try:
        res = get_with_retry(GBIF_SPECIES_URL, params)
        if not res:
            print(f"  ❌ Error: Failed after retries (network issue)")
            return {
                "success": False,
                "order": order_name,
                "order_key": order_key,
                "error": "Network failure"
            }
        
        results = res.get("results", [])
        total_available = res.get("count", 0)
        
        print(f"  📊 Total species available in GBIF: {total_available:,}")
        print(f"  📝 Retrieved {len(results)} samples")
        
        if results:
            print(f"\n  Sample species:")
            samples = []
            for i, r in enumerate(results[:3], 1):
                name = r.get("canonicalName", "Unknown")
                family = r.get("family", "Unknown")
                
                inat_data = fetch_inat_images(name)
                has_img = "🖼️" if inat_data["image_primary"] else "⚠️"

                print(f"    {i}. {name} ({family}) {has_img}")

                # 🔍 DEBUG: show what iNat actually returned
                if inat_data["inat_id"]:
                    print(f"       → iNat ID: {inat_data['inat_id']}")
                    print(f"       → Common: {inat_data['common_name']}")
                    print(f"       → Obs: {inat_data['observations_count']}")
                    print(f"       → Img: {inat_data['image_primary'][:60]}...")
                else:
                    print(f"       → ❌ No iNaturalist match")
                
                samples.append({
                    "order": order_name,
                    "scientificName": r.get("scientificName"),
                    "canonicalName": name,
                    "family": family,
                    "genus": r.get("genus"),
                    "gbif_id": r.get("key"),
                    
                    # iNaturalist
                    "inat_id": inat_data["inat_id"],
                    "common_name": inat_data["common_name"],
                    "observations_count": inat_data["observations_count"],
                    "image_primary": inat_data["image_primary"],
                    "image_gallery": str(inat_data["image_gallery"])
                })
            
            return {
                "success": True,
                "order": order_name,
                "order_key": order_key,
                "samples": samples,
                "total_available": total_available,
                "verification": verification
            }
        else:
            print(f"  ❌ No results found!")
            return {
                "success": False,
                "order": order_name,
                "order_key": order_key,
                "error": "No results"
            }
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return {
            "success": False,
            "order": order_name,
            "order_key": order_key,
            "error": str(e)
        }

def run_mini_extraction_test(limit_per_order=10):
    """
    Runs a mini extraction test with a small sample from each order.
    """
    print("=" * 70)
    print("🌸 CALYX MINI DATA EXTRACTION TEST")
    print("=" * 70)
    print(f"\nTesting {len(TARGET_ORDERS)} orders with {limit_per_order} species each")
    print(f"Total test sample size: ~{len(TARGET_ORDERS) * limit_per_order} species")
    
    all_results = []
    all_samples = []
    
    # Test each order
    for order_name, order_key in TARGET_ORDERS.items():
        result = test_order_extraction(order_name, order_key, test_limit=limit_per_order)
        all_results.append(result)
        
        if result["success"]:
            all_samples.extend(result["samples"])
        
        time.sleep(0.5)  # Be nice to GBIF API
    
    # Generate summary report
    print("\n" + "=" * 70)
    print("📊 EXTRACTION TEST SUMMARY")
    print("=" * 70)
    
    successful = [r for r in all_results if r["success"]]
    failed = [r for r in all_results if not r["success"]]
    
    print(f"\n✅ Successful: {len(successful)}/{len(TARGET_ORDERS)} orders")
    print(f"❌ Failed: {len(failed)}/{len(TARGET_ORDERS)} orders")
    
    if failed:
        print("\n⚠️  Failed orders:")
        for f in failed:
            print(f"  - {f['order']} (Key: {f['order_key']}): {f.get('error', 'Unknown error')}")
    
    if successful:
        # Calculate total available species
        total_available = sum(r["total_available"] for r in successful)
        print(f"\n📈 Total species available across all orders: {total_available:,}")
        
        # Family diversity check
        families = [s["family"] for s in all_samples if s.get("family")]
        family_counts = Counter(families)
        print(f"\n🌿 Family diversity in sample: {len(family_counts)} unique families")
        print(f"   Top 5 families:")
        for family, count in family_counts.most_common(5):
            print(f"     - {family}: {count} species")
        
        # Use a double-get or a conditional to safely check the nested dictionary
        angiosperms = [
            r for r in successful 
            if r and r.get("verification") and r["verification"].get("is_angiosperm")
        ]
        print(f"\n✓ Angiosperm verification: {len(angiosperms)}/{len(successful)} orders verified as flowering plants")
    
    return all_samples, all_results

def get_with_retry(url, params=None, retries=3):
    """Helper to handle intermittent network drops."""
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(2 ** i) # Exponential backoff
    return None

def sync_backbone_keys(order_names):
    """
    Dynamically fetches the correct GBIF Backbone keys for a list of names
    to prevent kingdom-mismatch collisions.
    """
    verified_orders = {}
    base_url = "https://api.gbif.org/v1/species/match"
    
    for name in order_names:
        params = {
            "name": name,
            "rank": "ORDER",
            "kingdom": "Plantae", # Forces the match to stay within the botanical realm
            "strict": True
        }
        response = requests.get(base_url, params=params).json()
        
        if not response:
            print(f"⚠️ Failed to resolve {name}")
            continue

        if (
            response.get("matchType") != "NONE"
            and response.get("kingdom") == "Plantae"
            and response.get("rank") == "ORDER"
        ):
            verified_orders[name] = response["orderKey"]
            print(f"✅ Verified {name}: {response['orderKey']}")
        else:
            print(f"⚠️ Could not resolve {name} in the Plantae kingdom.")
            
    return verified_orders

TARGET_ORDERS = sync_backbone_keys(ORDERS_NAMES) 

# Example usage for Calyx
target_order_names = ["Asparagales", "Liliales", "Poales", "Arecales", "Alismatales"]

def build_test_dataset(limit_per_order=10):
    """
    Builds a small test dataset with Wikipedia descriptions.
    """
    print("\n" + "=" * 70)
    print("🚀 BUILDING TEST DATASET WITH WIKIPEDIA DESCRIPTIONS")
    print("=" * 70)
    
    final_data = []
    
    for order_name, order_key in TARGET_ORDERS.items():
        print(f"\n📦 Processing: {order_name}")
        order_count = 0
        
        params = {
            "highertaxonKey": order_key,
            "datasetKey": "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c",
            "rank": "SPECIES",
            "status": "ACCEPTED",
            "limit": limit_per_order,
            "offset": 0
        }
        
        try:
            res = requests.get(GBIF_SPECIES_URL, params=params).json()
            results = res.get("results", [])
            
            for r in results:
                name = r.get("canonicalName")
                if name:
                    print(f"  → {name}...", end=" ")
                    
                    desc = fetch_wiki_description(name)
                    inat_data = fetch_inat_images(name)
                    
                    desc_status = "✓" if len(desc) > 50 else "⚠"
                    img_status = "🖼️" if inat_data["image_primary"] else "⚠"
                    
                    print(f"{desc_status} | {img_status}")
                    
                    final_data.append({
                        "order": order_name,
                        "scientificName": r.get("scientificName"),
                        "canonicalName": name,
                        "family": r.get("family"),
                        "genus": r.get("genus"),
                        "gbif_id": r.get("key"),
                        
                        # Wikipedia
                        "description": desc,
                        "description_length": len(desc),
                        "has_wiki": "Yes" if len(desc) > 50 else "No",
                        
                        # iNaturalist
                        "inat_id": inat_data["inat_id"],
                        "common_name": inat_data["common_name"],
                        "observations_count": inat_data["observations_count"],
                        "image_primary": inat_data["image_primary"],
                        "image_gallery": "|".join(inat_data["image_gallery"]),
                        
                        # Manual fields
                        "color_primary": "",
                        "native_region": ""
                    })
                    order_count += 1
            
            print(f"  ✅ Collected {order_count} species from {order_name}")
            time.sleep(0.2)  # Be nice to APIs
            
        except Exception as e:
            print(f"  ❌ Error in {order_name}: {e}")
    
    return final_data

if __name__ == "__main__":
    start_time = time.time()
    
    print("\n🌺 CALYX DATA ENGINE - MINI TEST MODE 🌺\n")
    
    # Step 1: Quick verification test (no Wikipedia calls)
    print("STEP 1: Quick Order Verification")
    print("-" * 70)
    samples, results = run_mini_extraction_test(limit_per_order=5)
    
    # Step 2: Ask if user wants to proceed with full mini extraction
    print("\n" + "=" * 70)
    proceed = input("\n✨ Verification complete! Proceed with full mini dataset (with Wikipedia)? (y/n): ")
    
    if proceed.lower() == 'y':
        # Build the test dataset
        test_data = build_test_dataset(limit_per_order=10)
        
        # Convert to DataFrame and Export
        df = pd.DataFrame(test_data)
        output_file = "calyx_mini_test.csv"
        df.to_csv(output_file, index=False)
        
        # Final summary
        duration = (time.time() - start_time) / 60
        print("\n" + "=" * 70)
        print("✨ TEST DATASET COMPLETE!")
        print("=" * 70)
        print(f"📁 File: {output_file}")
        print(f"📊 Total species: {len(df)}")
        print(f"🌐 With Wikipedia descriptions: {df['has_wiki'].value_counts().get('Yes', 0)}")
        print(f"⏱️  Time elapsed: {duration:.2f} minutes")
        print(f"\n💡 Review this file before running the full extraction on Oracle server!")
    else:
        print("\n👍 Test complete! Review the verification results above.")
    
    print("\n" + "=" * 70)

    print(fetch_wiki_description("Carica papaya"))