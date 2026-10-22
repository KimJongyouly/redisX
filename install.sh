#!/usr/bin/env bash
# Builds redisX's release binaries and installs them somewhere you can run
# without `cargo` — by default /usr/local/bin/redisX, which usually needs
# sudo to write to (first-time directory creation). NOT /usr/bin: on modern
# macOS that path lives on the SIP-protected system volume, so writing there
# fails with "Operation not permitted" even as root — sudo can't fix it, so
# we don't default to it. Override with INSTALL_DIR if you'd rather use
# somewhere else entirely, e.g.:
#   INSTALL_DIR="$HOME/.local/redisX" ./install.sh
#
# This script only ever builds and copies files — it never starts the
# server, writes to your shell rc files, or touches anything outside
# $INSTALL_DIR. The two follow-up steps it can't safely do for you (editing
# redisx.conf, adding $INSTALL_DIR to your PATH) are printed at the end
# instead.
#
# --force: wipe $INSTALL_DIR first and reinstall clean. For recovering from a
# directory left in a bad state by an earlier run (e.g. root-owned from an
# old script version that didn't hand ownership back — see git history), not
# for routine reinstalls, since a plain re-run already overwrites the three
# binaries in place. Confirms sudo access up front (before deleting anything)
# so a mistyped password doesn't leave you with $INSTALL_DIR removed and
# nothing to replace it.
#
# --clean: a full reset — implies --force (wipe $INSTALL_DIR, same as above)
# AND runs `cargo clean` on this repo's own target/ directory first, so the
# release binaries are rebuilt completely from scratch rather than reusing
# any cached build artifacts. Use this if you suspect a stale/corrupted build
# cache (e.g. a feature flag change — like this script's own python/polars
# features — that doesn't seem to be taking effect), not for routine
# reinstalls: it's much slower than a plain re-run or `--force` alone, since
# every dependency (including polars/pyo3/mlua) recompiles from zero.
#
# Always builds redisx-server with `--features python,polars` (pyo3 +
# polars), so an installed copy can run `FUNCTION LOAD`/`FCALL` Python
# libraries AND the native `DF.*` DataFrame commands out of the box — a
# plain `cargo build --release --workspace` here would silently omit both
# (they're opt-in at the crate level; see crates/redisx-server/Cargo.toml's
# `[features] python`/`polars`). `python` needs a Python 3 dev environment on
# the machine running install.sh (same requirement `cargo test --features
# python` has); `polars` has no extra system requirement (vendored, pure
# Cargo dependency) — there's no plain-build fallback here since the point of
# this change is that an installed copy always has both out of the box.
set -euo pipefail

FORCE=0
CLEAN=0
for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        --clean) FORCE=1; CLEAN=1 ;;
        *)
            echo "Usage: $0 [--force] [--clean]" >&2
            exit 1
            ;;
    esac
done

INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin/redisX}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$FORCE" -eq 1 ]; then
    echo "==> --force: confirming sudo access before touching $INSTALL_DIR ..."
    sudo -v
    if [ -e "$INSTALL_DIR" ]; then
        echo "==> Removing existing $INSTALL_DIR ..."
        sudo rm -rf "$INSTALL_DIR"
    fi
fi

if [ "$CLEAN" -eq 1 ]; then
    echo "==> --clean: running cargo clean before rebuilding from scratch ..."
    cargo clean
fi

echo "==> Building release binaries (cargo build --release --workspace --features redisx-server/python,redisx-server/polars)..."
cargo build --release --workspace --features redisx-server/python,redisx-server/polars

maybe_sudo_mkdir() {
    if [ -d "$INSTALL_DIR" ] && [ -w "$INSTALL_DIR" ]; then
        return
    fi
    if mkdir -p "$INSTALL_DIR" 2>/dev/null && [ -w "$INSTALL_DIR" ]; then
        return
    fi
    echo "==> $INSTALL_DIR isn't writable without sudo; asking for it now..."
    sudo mkdir -p "$INSTALL_DIR"
    # Hand ownership to the invoking user (covers both a brand-new directory
    # and a pre-existing one left root-owned by an earlier sudo run) so that
    # redisx-server, run later as a normal user, can write its own
    # redisx.conf here on first launch without needing sudo every time.
    sudo chown "$(id -u):$(id -g)" "$INSTALL_DIR"
}

install_one() {
    local src="$1" dst="$INSTALL_DIR/$2"
    if [ -w "$INSTALL_DIR" ]; then
        cp "$src" "$dst"
    else
        sudo cp "$src" "$dst"
    fi
}

echo "==> Installing to $INSTALL_DIR ..."
maybe_sudo_mkdir
install_one target/release/redisx-server redisx-server
install_one target/release/redisx-cli redisx-cli
install_one target/release/redisx-benchmark redisx-benchmark

CONFIG_PATH="$INSTALL_DIR/redisx.conf"

cat <<EOF

==================================================================
redisX installed to: $INSTALL_DIR
  redisx-server, redisx-cli, redisx-benchmark

These are now plain standalone binaries — no cargo needed to run them.
redisx-server was built with Python scripting support (FUNCTION LOAD/FCALL)
and native DataFrame support (DF.FROMCSV/DF.SET/DF.GET/DF.TOCSV/DF.SHAPE/
DF.COLUMNS/DF.QUERY).

[1] 설정 파일(redisx.conf)은 redisx-server를 처음 실행할 때
    $INSTALL_DIR 안에 기본값으로 자동 생성됩니다.
    세부 설정(포트, 메모리 제한 등)을 바꾸려면 다음 파일을 수정한 뒤
    서버를 재시작하세요:
      $CONFIG_PATH

[2] cargo나 전체 경로 없이 어디서든 redisx-server / redisx-cli를
    실행하려면, 아래 한 줄을 ~/.zshrc (zsh 사용 시) 또는 ~/.shrc
    (sh/bash 사용 시)에 추가한 뒤 터미널을 새로 열거나
    'source ~/.zshrc' 등을 실행하세요:

      export PATH="$INSTALL_DIR:\$PATH"
==================================================================
EOF
