# Use lightweight Python
FROM python:3.11-slim

# Prevent Python from buffering logs (important for AWS logs)
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system deps (minimal)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for caching)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Default command (can override in docker run)
CMD ["python", "calyx_production.py", "--batch", "1", "--limit", "5000"]