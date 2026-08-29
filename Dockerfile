FROM ghcr.io/anomalyco/opencode

# Install runtime deps for running PySpark locally (unit tests):
#   - openjdk17-jre-headless: Spark requires a JVM (java gateway)
#   - bash: Spark's launch scripts invoke bash, which Alpine omits by default
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 py3-pip openjdk17-jre-headless bash

# Install python packages
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade --break-system-packages pip \
    && pip install --break-system-packages -r requirements.txt \
    && rm requirements.txt

RUN useradd -u 1000 -m opencode
USER opencode