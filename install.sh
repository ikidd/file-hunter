#!/usr/bin/env bash
set -euo pipefail

# ── Install File Hunter ──────────────────────────────────────────────
#
# curl -fsSL https://filehunter.zenlogic.uk/install | bash
#   or
# curl -fsSL https://raw.githubusercontent.com/zen-logic/file-hunter/main/install.sh | bash

REPO="zen-logic/file-hunter"

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
error() { printf '\033[1;31mError:\033[0m %s\n' "$*" >&2; exit 1; }

# ── Get latest release tag ───────────────────────────────────────────

info "Fetching latest release..."
TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')

if [ -z "$TAG" ]; then
    error "Could not determine latest release."
fi

VERSION="${TAG#v}"
ARCHIVE="filehunter-${VERSION}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${TAG}/${ARCHIVE}"

info "Downloading File Hunter ${VERSION}..."
curl -fSL "$URL" -o "/tmp/${ARCHIVE}" || error "Download failed."

# ── Install location ─────────────────────────────────────────────────

printf '  Install location [./filehunter]: '
read -r INSTALL_DIR </dev/tty
INSTALL_DIR="${INSTALL_DIR:-./filehunter}"

# ── Extract ──────────────────────────────────────────────────────────

TMPDIR=$(mktemp -d)
tar xzf "/tmp/${ARCHIVE}" -C "$TMPDIR"
rm -f "/tmp/${ARCHIVE}"

EXTRACTED="$TMPDIR/filehunter-${VERSION}"
if [ ! -d "$EXTRACTED" ]; then
    rm -rf "$TMPDIR"
    error "Unexpected archive structure."
fi

if [ -d "$INSTALL_DIR" ]; then
    # Existing install — update in place, preserving user data
    info "Updating existing installation at ${INSTALL_DIR}..."
    for dir in file_hunter file_hunter_core file_hunter_agent static; do
        rm -rf "${INSTALL_DIR:?}/$dir"
    done
    for item in "$EXTRACTED"/*; do
        name=$(basename "$item")
        case "$name" in
            data|config.json|venv) continue ;;
        esac
        cp -R "$item" "$INSTALL_DIR/"
    done
    chmod +x "$INSTALL_DIR/filehunter"
    ACTION="updated"
else
    # Fresh install
    mv "$EXTRACTED" "$INSTALL_DIR"
    ACTION="installed"
fi

rm -rf "$TMPDIR"

# ── Done ─────────────────────────────────────────────────────────────

echo ""
info "File Hunter ${VERSION} ${ACTION}."
echo ""
echo "  cd ${INSTALL_DIR}"
echo "  ./filehunter"
echo ""
