FROM ghcr.io/anomalyco/opencode

# Install runtime deps for running PySpark locally (unit tests):
#   - openjdk17-jre-headless: Spark requires a JVM (java gateway)
#   - bash: Spark's launch scripts invoke bash, which Alpine omits by default
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 py3-pip openjdk17-jre-headless bash nodejs npm unzip curl libxml2-utils git github-cli
RUN curl -fsSLO https://releases.hashicorp.com/terraform/1.16.0/terraform_1.16.0_linux_amd64.zip \
    && unzip terraform_1.16.0_linux_amd64.zip \
    && mv terraform /usr/bin

# draw.io Desktop CLI (drawio) for the drawio skill (.opencode/skills/drawio).
# drawio-desktop is only packaged in Alpine's edge/testing repo, so enable the
# edge main/community/testing repos in addition to the v3.24 stable repos.
# It is an Electron app, so headless CLI use (Mermaid -> .drawio, ELK --layout,
# PNG/SVG/PDF export) requires an X server (xvfb) plus fontconfig and a font so
# exported diagrams render text.
# ELECTRON_DISABLE_SANDBOX is required: as an Electron app it tries to create
# namespaces, which is blocked in this container. Without it drawio dies with a
# zygote/namespace check failure. xvfb / xvfb-run provide the virtual X display.
ENV ELECTRON_DISABLE_SANDBOX=1
RUN echo "https://dl-cdn.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories \
    && echo "https://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
    && echo "https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
    && apk add --update --no-cache drawio-desktop xvfb xvfb-run fontconfig ttf-dejavu
RUN npx @drawio/mcp

# Install python packages
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade --break-system-packages pip \
    && pip install --break-system-packages -r requirements.txt \
    && rm requirements.txt

RUN adduser -u 1000 -D opencode
USER opencode
