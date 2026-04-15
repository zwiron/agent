#!/bin/sh
set -eu

# Zwiron Agent installer
# Usage: curl -fsSL https://raw.githubusercontent.com/zwiron/agent/main/install.sh | sh

REPO="zwiron/agent"
BINARY="zwiron-agent"
INSTALL_DIR="/usr/local/bin"

main() {
    check_root
    detect_platform
    get_latest_version
    download_and_install
    echo ""
    echo "  zwiron-agent ${VERSION} installed to ${INSTALL_DIR}/${BINARY}"
    echo ""
    echo "  Next steps:"
    echo "    sudo zwiron-agent install --token <TOKEN> --addr <ATLAS_ADDR>:9090"
    echo ""
}

check_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Error: this installer must be run as root (use sudo)." >&2
        exit 1
    fi
}

detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "${OS}" in
        linux)  OS="linux" ;;
        darwin) OS="darwin" ;;
        *)
            echo "Error: unsupported operating system: ${OS}" >&2
            exit 1
            ;;
    esac

    case "${ARCH}" in
        x86_64|amd64)  ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *)
            echo "Error: unsupported architecture: ${ARCH}" >&2
            exit 1
            ;;
    esac

    echo "Detected platform: ${OS}/${ARCH}"
}

get_latest_version() {
    if [ -n "${ZWIRON_VERSION:-}" ]; then
        VERSION="${ZWIRON_VERSION}"
        echo "Using specified version: ${VERSION}"
        return
    fi

    echo "Fetching latest release..."
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' \
        | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')

    if [ -z "${VERSION}" ]; then
        echo "Error: could not determine latest version." >&2
        echo "Set ZWIRON_VERSION=v1.0.0 to install a specific version." >&2
        exit 1
    fi

    echo "Latest version: ${VERSION}"
}

download_and_install() {
    TARBALL="${BINARY}-${VERSION}-${OS}-${ARCH}.tar.gz"
    URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"

    TMPDIR=$(mktemp -d)
    trap 'rm -rf "${TMPDIR}"' EXIT

    echo "Downloading ${TARBALL}..."
    curl -fsSL "${URL}" -o "${TMPDIR}/${TARBALL}"

    echo "Verifying checksum..."
    CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"
    curl -fsSL "${CHECKSUMS_URL}" -o "${TMPDIR}/checksums.txt"

    EXPECTED=$(grep "${TARBALL}" "${TMPDIR}/checksums.txt" | awk '{print $1}')
    if [ -z "${EXPECTED}" ]; then
        echo "Warning: checksum not found, skipping verification." >&2
    else
        if command -v sha256sum >/dev/null 2>&1; then
            ACTUAL=$(sha256sum "${TMPDIR}/${TARBALL}" | awk '{print $1}')
        else
            ACTUAL=$(shasum -a 256 "${TMPDIR}/${TARBALL}" | awk '{print $1}')
        fi

        if [ "${EXPECTED}" != "${ACTUAL}" ]; then
            echo "Error: checksum mismatch!" >&2
            echo "  Expected: ${EXPECTED}" >&2
            echo "  Actual:   ${ACTUAL}" >&2
            exit 1
        fi
        echo "Checksum verified."
    fi

    echo "Installing to ${INSTALL_DIR}/${BINARY}..."
    tar xzf "${TMPDIR}/${TARBALL}" -C "${TMPDIR}"
    install -m 755 "${TMPDIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
}

main
