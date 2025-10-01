# Multi-arch IoT Edge Python base (ARM64 supported)
FROM mcr.microsoft.com/azureiotedge/python:3.10-bullseye

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# Native deps (kept lean)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ make cmake \
    libssl-dev libffi-dev uuid-dev zlib1g-dev \
    libcurl4-openssl-dev pkg-config git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Force a prebuilt wheel for uamqp on ARM64 with Py3.10, then install the rest
RUN pip install --upgrade pip setuptools wheel && \
    pip install --only-binary=:all: uamqp==1.6.8 && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python","-u","main.py"]
