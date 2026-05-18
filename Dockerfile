# Use a stable Ubuntu base image
FROM ubuntu:22.04

# Install C++ compiler tools and clean up afterward to keep the image small
RUN apt-get update && apt-get install -y \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# SECURITY: Create a non-root user. We DO NOT want untrusted code running as root!
RUN useradd -m sandboxuser
USER sandboxuser

# Set the directory where the contestant's code will be copied
WORKDIR /app
