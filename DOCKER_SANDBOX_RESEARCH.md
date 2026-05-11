# Research: Docker Sandboxing for C++ Submissions

To ensure fair resource allocation and prevent malicious code execution (as required by the IICPC specifications), we need to run contestant binaries in heavily restricted Docker containers. 

Here are the critical `docker run` flags we will need to implement:

## 1. Resource Limits (Fairness)

* **Memory Limit (`--memory` or `-m`):** Hard limit on RAM. If the C++ code exceeds this, the container gets a kill signal (OOMKilled).
    * *Example:* `--memory="256m"`
* **CPU Limit (`--cpus`):** Limits how much of the host's CPU the container can use. Setting this to `1.0` means the container can use at most one full CPU core.
    * *Example:* `--cpus="1.0"`

## 2. Security & Isolation (Sandboxing)

Since we are running untrusted third-party code, resource limits aren't enough. We also need to lock down the container's permissions:

* **Disable Networking (`--network none`):** Prevents the contestant's code from downloading malicious payloads or launching DDoS attacks.
* **Read-Only Filesystem (`--read-only`):** Prevents the code from modifying the container's internal OS files.
* **Drop Privileges (`--security-opt=no-new-privileges`):** Ensures the process cannot escalate its privileges, even if there's a vulnerability.

## 3. The Ultimate Execution Command

When the backend triggers a run for a compiled C++ binary (let's assume it's named `contestant_bot`), the command spawned by our engine should look something like this:

```bash
docker run --rm \
  --memory="256m" \
  --cpus="1.0" \
  --network none \
  --read-only \
  --security-opt=no-new-privileges \
  -v /path/to/compiled/binary:/app/contestant_bot:ro \
  ubuntu:22.04 /app/contestant_bot
