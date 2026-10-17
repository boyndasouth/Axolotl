# Sandbox image: only what the pipeline code needs. No project code is baked in;
# each verification mounts a throwaway copy at /work and runs with --network none.
#
#   docker build -t shp-sandbox:latest sandbox/
FROM python:3.12-slim

RUN pip install --no-cache-dir "pandas>=2.2" "pandera>=0.20" "pyyaml>=6" "pytest>=8"

RUN useradd --create-home --uid 10001 sandbox
USER sandbox
WORKDIR /work
CMD ["python", "entry.py"]
