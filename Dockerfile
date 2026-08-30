FROM ghcr.io/anomalyco/opencode

# Install runtime deps for running PySpark locally (unit tests):
#   - openjdk17-jre-headless: Spark requires a JVM (java gateway)
#   - bash: Spark's launch scripts invoke bash, which Alpine omits by default
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 py3-pip openjdk17-jre-headless bash nodejs npm unzip curl
RUN curl -fsSLO https://releases.hashicorp.com/terraform/1.16.0/terraform_1.16.0_linux_amd64.zip \
    && unzip terraform_1.16.0_linux_amd64.zip \
    && mv terraform /usr/bin

# Install python packages
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade --break-system-packages pip \
    && pip install --break-system-packages -r requirements.txt \
    && rm requirements.txt

RUN adduser -u 1000 -D opencode
USER opencode