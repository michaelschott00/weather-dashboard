FROM ghcr.io/anomalyco/opencode

ENV OPENCODE_CONFIG_CONTENT='{"$schema":"https://opencode.ai/config.json","lsp":true}'

# Install python
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 py3-pip

# Install python packages
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade --break-system-packages pip \
    && pip install --break-system-packages -r requirements.txt \
    && rm requirements.txt