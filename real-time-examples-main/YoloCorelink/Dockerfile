FROM nvidia/cuda:12.1.0-cudnn8-devel-ubuntu22.04

# Set the working directory in the container
WORKDIR /usr/src/app

# Set environment variables to configure tzdata non-interactively
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Install Python 3.10 and pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 \
    python3.10-venv \
    python3.10-dev \
    python3-pip \
    build-essential \
    git \
    libsm6 \
    libxext6 \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libxrender1

# Install Python packages
COPY requirements.txt .
RUN pip3 install --upgrade pip setuptools six && \
    pip3 install --no-cache-dir -r requirements.txt

# Set LD_LIBRARY_PATH to include CUDA and cuDNN libraries
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/cuda/extras/CUPTI/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/lib/x86_64-linux-gnu"

# Copy the script and data files
COPY detectionReceiver.py .
COPY yolov10x.pt . 

# The command to run the script, you'll overwrite this in Kubernetes deployment
CMD ["python3", "./detectionReceiver.py"]
