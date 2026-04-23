# 🚀 Calyx Pipeline Quickstart (Google Cloud + Docker)

This guide gets you from zero → running pipeline on a Google server.

---

## 1️⃣ Prerequisites

- Google Cloud account
- Billing enabled
- gcloud CLI installed

---

## 2️⃣ Create VM Instance
gcloud compute instances create calyx-runner \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --boot-disk-size=50GB \
  --image-family=debian-11 \
  --image-project=debian-cloud

---

## 3️⃣ SSH Into Server

gcloud compute ssh calyx-runner

---

## 4️⃣ Install Docker

sudo apt update
sudo apt install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker

Optional (no sudo):

sudo usermod -aG docker $USER
newgrp docker

---

## 5️⃣ Upload Your Project

From local machine:

gcloud compute scp --recurse ./calyx-project calyx-runner:~/

SSH back in:

cd ~/calyx-project

---

## 6️⃣ Build Docker Image

docker build -t calyx-pipeline .

---

## 7️⃣ Run Pipeline (Basic)

docker run -it calyx-pipeline

---

## 8️⃣ Run With Persistent Storage (IMPORTANT)

docker run -it \
  -v $(pwd)/data:/app \
  calyx-pipeline

This preserves:

CSV output
checkpoint file
cache

---

## 9️⃣ Resume Mode (CRITICAL)

docker run -it \
  -v $(pwd)/data:/app \
  calyx-pipeline \
  python calyx_production.py --batch 1 --resume

---

## 🔟 Production Run (Recommended)

docker run -d \
  -v $(pwd)/data:/app \
  calyx-pipeline \
  python calyx_production.py --batch 1 --limit 200000 --resume
📊 Monitor Logs
docker logs -f <container_id>
🧯 Stop Container
docker stop <container_id>
📁 Output Files

Inside /data:

calyx_species_data.csv
calyx_checkpoint.json
wiki_cache.json
⚙️ Tuning Tips
Parameter	Effect
--limit	species per order
INAT_DELAY	API stability
INAT_WORKERS	speed vs rate-limit
chunk_size	memory vs throughput

## 🚨 Common Issues
1. No images
Normal for rare species
2. Slow performance
Reduce INAT_DELAY slightly
Increase VM size
3. Restart needed
Use --resume

💡 Recommended VM Specs
Scale	Machine
Testing	e2-standard-2
Production	e2-standard-4+
Heavy runs	e2-highmem-8

🧠 Pro Tip

Run overnight with:

--resume + high limit

The pipeline is built for long-haul extraction.


✅ You're Good To Go