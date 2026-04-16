# 🚀 Quick Start Guide

## Installation (30 seconds)

```bash
# Install dependencies
pip install aiohttp pandas requests

# Download script
# (already have calyx_production.py)

# Ready to run!
```

## First Run (5 minutes)

### Test with Small Sample
```bash
python calyx_production.py --batch 1 --limit 100
```

**Expected output:**
```
🌺 CALYX PRODUCTION DATA EXTRACTION PIPELINE 🌺
Output: calyx_species_data.csv
Checkpoint: calyx_checkpoint.json
Limit per order: 100

🔍 Resolving 2 order keys from GBIF Backbone...
  ✅ Asparagales: 769
  ✅ Liliales: 785

============================================================
Processing Order: Asparagales
============================================================
🌸 Fetching species for Asparagales (Key: 769)
  → Fetched 100 new species (offset: 100)
✅ Fetched 100 species from Asparagales
🔄 Processing 100 species with Wikipedia enrichment...
  ✓ Processed 100/100 species
✅ Completed Asparagales: 100 species

✨ EXTRACTION COMPLETE!
📁 Output: calyx_species_data.csv
⏱️  Time: 2.5 minutes
```

### Check Output
```bash
# View first 10 rows
head -20 calyx_species_data.csv

# Count rows
wc -l calyx_species_data.csv
```

## Production Run

### Single Batch (~2 hours, ~10k species)
```bash
python calyx_production.py --batch 1 --limit 5000
```

### All Batches (~50 hours, ~300k species)
```bash
# Run in background
nohup python calyx_production.py --all --limit 50000 > extraction.log 2>&1 &

# Monitor progress
tail -f extraction.log

# Check job status
ps aux | grep calyx
```

## Resume After Interruption

```bash
# Just re-run the same command
python calyx_production.py --batch 1 --limit 5000

# Automatic resume - skips processed species
```

## Files Generated

```
calyx_species_data.csv       ← YOUR DATA
calyx_checkpoint.json        ← Resume state
wiki_cache.json              ← Speed optimization
extraction.log               ← Detailed logs (optional)
```

## Quick Commands

```bash
# Count processed species
wc -l calyx_species_data.csv

# View checkpoint status
cat calyx_checkpoint.json | python -m json.tool

# Check Wikipedia cache size
du -h wiki_cache.json

# Monitor real-time
watch -n 5 'wc -l calyx_species_data.csv'
```

## Common Patterns

### Extract Specific Orders
```bash
python calyx_production.py --orders Asterales Rosales Fabales --limit 10000
```

### Custom Output Location
```bash
python calyx_production.py \
  --batch 3 \
  --limit 15000 \
  --output /data/flowers_batch3.csv \
  --checkpoint /data/checkpoint_b3.json
```

### With Logging
```bash
python calyx_production.py \
  --all \
  --limit 50000 \
  --log-file full_extraction.log
```

## Performance Tips

**For Oracle Free Server:**
- Use `--limit 5000` per order (safe)
- Run one batch at a time
- Monitor disk space: `df -h`

**For Dedicated Server:**
- Use `--limit 15000` per order
- Can handle `--all` flag
- Increase Wikipedia workers (edit script)

## Need Help?

1. **Check logs:** `cat extraction.log`
2. **Verify checkpoint:** `cat calyx_checkpoint.json`
3. **Count records:** `wc -l calyx_species_data.csv`
4. **Restart:** Just re-run the command (auto-resumes)

---

That's it! You're ready to extract. 🌸
