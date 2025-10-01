# Use full image (has more build tools than slim)
FROM python:3.11

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# System libs needed for uamqp build on ARM64
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc g++ make cmake \
    uuid-dev \
    libssl-dev \
    libffi-dev \
    libcurl4-openssl-dev \
    pkg-config \
    zlib1g-dev \
    git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Upgrade build tooling, preinstall a compatible uamqp wheel, then the rest
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uamqp==1.6.8 && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python","-u","main.py"]
