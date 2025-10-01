FROM python:3.11-slim

# Keep Python output unbuffered & don’t write .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 1) Install system packages needed to build uamqp (pulled by azure-iot-device)
#    - build-essential: gcc, g++, make
#    - libssl-dev, libffi-dev: crypto headers for OpenSSL
#    - libcurl4-openssl-dev: required by underlying libs on some platforms
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 2) Install Python deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 3) Copy app code
COPY main.py .

# 4) Start
CMD ["python","-u","main.py"]

