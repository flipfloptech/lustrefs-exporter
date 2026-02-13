# ── Build stage ──────────────────────────────────────────────────────────
ARG BASE_IMAGE=rockylinux:8.10
FROM ${BASE_IMAGE} AS builder

# System deps: C toolchain, git (for git deps in Cargo.toml), cmake (zstd-sys)
RUN dnf install -y \
        gcc gcc-c++ make cmake \
        git \
        openssl-devel \
        zlib-devel \
    && dnf clean all

# Install Rust via rustup (edition 2024 requires >= 1.85)
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH="/usr/local/cargo/bin:${PATH}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal \
    && rustc --version

WORKDIR /build

# Cache dependency builds: copy manifests first, then do a dummy build
COPY Cargo.toml Cargo.lock ./
COPY lustre-collector/Cargo.toml lustre-collector/Cargo.toml
COPY lustrefs-exporter/Cargo.toml lustrefs-exporter/Cargo.toml
COPY lustrefs-loadtest/Cargo.toml lustrefs-loadtest/Cargo.toml

# Create stub lib/main files so cargo can resolve the workspace
RUN mkdir -p lustre-collector/src lustrefs-exporter/src lustrefs-loadtest/src \
    && echo 'pub fn stub() {}' > lustre-collector/src/lib.rs \
    && echo 'fn main() {}' > lustrefs-exporter/src/main.rs \
    && echo 'pub fn stub() {}' > lustrefs-exporter/src/lib.rs \
    && echo 'fn main() {}' > lustrefs-loadtest/src/main.rs \
    && cargo build --release -p lustrefs-exporter 2>/dev/null || true \
    && rm -rf lustre-collector/src lustrefs-exporter/src lustrefs-loadtest/src

# Copy real source and build
COPY . .
RUN cargo build --release -p lustrefs-exporter

# ── Runtime stage ────────────────────────────────────────────────────────
FROM ${BASE_IMAGE}

# Only the shared libs needed at runtime (zstd, zlib, openssl)
RUN dnf install -y \
        zlib \
        openssl-libs \
    && dnf clean all

COPY --from=builder /build/target/release/lustrefs-exporter /usr/local/bin/lustrefs-exporter

EXPOSE 32221

ENTRYPOINT ["lustrefs-exporter"]
