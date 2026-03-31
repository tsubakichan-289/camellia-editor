#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_TRIPLE="${TARGET_TRIPLE:-x86_64-pc-windows-gnu}"
PROFILE="${PROFILE:-release}"
APP_DIR="${ROOT_DIR}/camellia-editor"
EXE_NAME="camellia-editor.exe"
TARGET_EXE="${ROOT_DIR}/target/${TARGET_TRIPLE}/${PROFILE}/${EXE_NAME}"
ISCC_EXE="${ISCC_EXE:-/mnt/c/Users/jyanto/AppData/Local/Programs/Inno Setup 6/ISCC.exe}"
ISS_FILE="${ROOT_DIR}/camellia-editor.iss"

echo "[1/4] cargo build --${PROFILE} --target ${TARGET_TRIPLE}"
cargo build --"${PROFILE}" --target "${TARGET_TRIPLE}"

if [[ ! -f "${TARGET_EXE}" ]]; then
  echo "missing target exe: ${TARGET_EXE}" >&2
  exit 1
fi

echo "[2/4] update packaged exe"
install -Dm755 "${TARGET_EXE}" "${APP_DIR}/${EXE_NAME}"

echo "[3/5] sync bundled templates"
rm -rf "${APP_DIR}/templates"
if [[ -d "${ROOT_DIR}/templates" ]]; then
  mkdir -p "${APP_DIR}"
  cp -R "${ROOT_DIR}/templates" "${APP_DIR}/templates"
fi

echo "[4/5] verify installer inputs"
required_files=(
  "${APP_DIR}/${EXE_NAME}"
  "${APP_DIR}/hunspell.exe"
  "${APP_DIR}/texlab.exe"
  "${APP_DIR}/en_US.aff"
  "${APP_DIR}/en_US.dic"
  "${APP_DIR}/templates"
)

for file in "${required_files[@]}"; do
  if [[ ! -e "${file}" ]]; then
    echo "missing required file: ${file}" >&2
    exit 1
  fi
done

if [[ ! -x "${ISCC_EXE}" && ! -f "${ISCC_EXE}" ]]; then
  echo "ISCC.exe not found: ${ISCC_EXE}" >&2
  exit 1
fi

echo "[5/5] build installer"
if command -v wslpath >/dev/null 2>&1; then
  ISS_FILE="$(wslpath -w "${ISS_FILE}")"
fi
"${ISCC_EXE}" "${ISS_FILE}"

echo
echo "installer ready:"
echo "  ${ROOT_DIR}/installer-dist/camellia-editor-setup.exe"
