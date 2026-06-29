import pandas as pd
import os

def analyze_large_species_csv(file_path):
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at absolute path: {os.path.abspath(file_path)}")
        print("Please verify that 'final_data' folder exists in the same directory as 'photos.py'")
        return

    print("⚡ Loading and processing your 205MB CSV file... Please wait...")
    
    # Read the CSV efficiently
    df = pd.read_csv(file_path)
    total_rows = len(df)
    
    if total_rows == 0:
        print("⚠️ Warning: The CSV file is empty.")
        return
    
    # Helper to clean column lookups (lowercasing to avoid case-sensitivity issues)
    df.columns = [col.lower().strip() for col in df.columns]
    
    # 1. Dataset Breakdown & Structure
    species_count = df['scientific_name'].nunique() if 'scientific_name' in df.columns else df['species'].nunique() if 'species' in df.columns else 0
    order_count = df['order'].nunique() if 'order' in df.columns else 0
    family_count = df['family'].nunique() if 'family' in df.columns else 0
    genus_count = df['genus'].nunique() if 'genus' in df.columns else 0
    
    # Check species per order matrix symmetry
    matrix_symmetry = "Symmetric"
    if order_count > 0 and species_count > 0:
        counts_per_order = df.groupby('order')['scientific_name'].nunique() if 'scientific_name' in df.columns else df.groupby('order').size()
        if counts_per_order.nunique() == 1:
            matrix_symmetry = f"Perfect Grid ({counts_per_order.iloc[0]} species per order)"
        else:
            matrix_symmetry = f"Asymmetric (Ranges from {counts_per_order.min()} to {counts_per_order.max()} species per order)"

    # 2. The Gaps
    missing_common = df['common_name'].isna().sum() if 'common_name' in df.columns else total_rows
    missing_obs = df['observation_count'].isna().sum() if 'observation_count' in df.columns else total_rows
    
    # 3. Image Profile Analysis
    img_col = next((col for col in ['image_url', 'image', 'url', 'media_url'] if col in df.columns), None)
    if img_col:
        has_images_count = df[img_col].notna().sum()
        image_pct = (has_images_count / total_rows) * 100
    else:
        has_images_count = 0
        image_pct = 0.0

    # Source parsing
    source_breakdown = {}
    if 'source' in df.columns and img_col:
        # Only look at sources where an image actually exists
        valid_img_sources = df[df[img_col].notna()]['source']
        if len(valid_img_sources) > 0:
            source_breakdown = (valid_img_sources.value_counts(normalize=True) * 100).to_dict()

    # --- PRINT THE CUSTOM AUDIT REPORT ---
    print("\n" + "="*60)
    print("📋 SYSTEM AUDIT REPORT: LARGE TAXA DATASET")
    print("="*60)
    
    print("\n## 1. Dataset Breakdown & Structure")
    print(f"* **Total Matrix Rows:** {total_rows:,} records detected.")
    print(f"* **Taxonomic Breadth:** {species_count:,} unique species, {genus_count:,} unique genera, {family_count:,} distinct families, and {order_count:,} targeted orders.")
    print(f"* **Grid Alignment:** {matrix_symmetry}")
    
    print("\n## 2. The Gaps (The Missing Links)")
    print(f"* **common_name:** {missing_common:,} / {total_rows:,} missing ({((missing_common/total_rows)*100):.1f}% unpopulated).")
    print(f"* **observation_count:** {missing_obs:,} / {total_rows:,} missing ({((missing_obs/total_rows)*100):.1f}% unpopulated).")
    
    print("\n## 3. The Current Image Profile")
    print(f"* **Image Fill Rate:** {has_images_count:,} species have image URLs (sitting at **{image_pct:.1f}%** coverage).")
    
    if source_breakdown:
        print("* **Source Factor Breakdown:**")
        for source, pct in source_breakdown.items():
            print(f"    - \"source\": \"{source}\" accounts for **{pct:.1f}%** of active images.")
            if 'wiki' in str(source).lower() and pct > 50:
                print(f"      > ⚠️ LEGAL RISK WARNING: High concentration of Wikipedia Commons media. Requires a CC-BY-SA due diligence audit prior to commercial exit.")
    else:
        print("* **Source Factor:** No image source metadata or active links could be mapped.")
        
    print("\n" + "="*60)

# --- EXECUTION ---
if __name__ == "__main__":
    # Dynamically find the path relative to this script file location
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join("Final_Data", "calyx_species_data.csv")
    
    analyze_large_species_csv(csv_file_path)