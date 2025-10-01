# ARM64 IoT Edge python base (Debian bookworm, ARM64v8)
FROM mcr.microsoft.com/azureiotedge/python:3.11-bookworm-arm64v8

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# (Usually already present, but safe to include the dev headers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev libffi-dev uuid-dev cmake pkg-config git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Force a prebuilt wheel for uamqp on ARM64 (manylinux2014_aarch64 exists)
RUN pip install --upgrade pip setuptools wheel && \
    pip install --only-binary=:all: uamqp==1.6.8 && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python","-u","main.py"]
