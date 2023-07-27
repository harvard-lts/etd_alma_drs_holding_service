import os
import sys
from pathlib import Path
# readiness probe for docker/k8s
# this code is from:
# https://medium.com/ambient-innovation/health-checks-for-celery-in-kubernetes-cf3274a3e106

ready_path = os.getenv("READINESS_FILE", "/tmp/worker_ready")
READINESS_FILE = Path(ready_path)

if not READINESS_FILE.is_file():
    print("worker NOT ready")
    sys.exit(1)
print("worker ready")
sys.exit(0)
