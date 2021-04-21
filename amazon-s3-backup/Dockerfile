ARG BUILD_FROM=hassioaddons/base:9.2.0
# hadolint ignore=DL3006
FROM ${BUILD_FROM}

# Copy root filesystem
COPY rootfs /

# Setup base
RUN apk add --no-cache \
    python3 \
    py3-boto3@edge \
    py3-watchdog \
    py3-requests

# Build arguments
ARG BUILD_ARCH
ARG BUILD_DATE
ARG BUILD_REF
ARG BUILD_VERSION

# Labels
LABEL \
    io.hass.name="Amazon S3 Backup" \
    io.hass.description="Automatically upload Home Assistant snapshots to Amazon S3" \
    io.hass.arch="${BUILD_ARCH}" \
    io.hass.type="addon" \
    io.hass.version=${BUILD_VERSION} \
    maintainer="Greg Rapp <gdrapp@gmail.com>" \
    org.opencontainers.image.title="Amazon S3 Backup" \
    org.opencontainers.image.description="Automatically upload Home Assistant snapshots to Amazon S3" \
    org.opencontainers.image.vendor="Home Assistant Community Add-ons" \
    org.opencontainers.image.authors="Greg Rapp <gdrapp@gmail.com>" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.url="https://github.com/gdrapp/hass-addons     " \
    org.opencontainers.image.source="https://github.com/gdrapp/hass-addons" \
    org.opencontainers.image.documentation="https://github.com/hass-addons/amazon-s3-backup/blob/master/README.md" \
    org.opencontainers.image.created=${BUILD_DATE} \
    org.opencontainers.image.revision=${BUILD_REF} \
    org.opencontainers.image.version=${BUILD_VERSION}