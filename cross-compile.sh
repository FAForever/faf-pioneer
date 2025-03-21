#!/bin/bash
set -e

OUTPUT_DIR="output"
mkdir -p "$OUTPUT_DIR"

build_target() {
    TARGET_CMD=$1
    OUTPUT_NAME=$2
    PLATFORM=$3
    TARGET_OS=$4
    TARGET_ARCH=$5

    TMP_DIR="${OUTPUT_DIR}/temp_build_${TARGET_OS}_${TARGET_ARCH}"
    mkdir -p "$TMP_DIR"
    DEST_FILE="${OUTPUT_DIR}/${OUTPUT_NAME}-${TARGET_OS}-${TARGET_ARCH}"

    echo "Building '${TARGET_CMD}' for ${PLATFORM} ($TARGET_OS/$TARGET_ARCH)..."
    docker buildx build \
      -f ./cmd/"${TARGET_CMD}"/Dockerfile \
      --platform "${PLATFORM}" \
      --build-arg TARGET_OS="${TARGET_OS}" \
      --build-arg TARGET_ARCH="${TARGET_ARCH}" \
      --target app \
      --output type=local,dest="${TMP_DIR}" \
      .

    if [ "${TARGET_OS}" = "windows" ]; then
      mv "${TMP_DIR}/${OUTPUT_NAME}" "${DEST_FILE}.exe"
    else
      mv "${TMP_DIR}/${OUTPUT_NAME}" "${DEST_FILE}"
    fi

    rm -rf "$TMP_DIR"
}

# Build for linux/amd64
#build_target "faf-adapter" "linux/amd64" "linux" "amd64"
#
## Build for MacOS darwin/amd64 (Intel) and darwin/arm64 (Apple Silicon)
#build_target "faf-adapter" "linux/amd64" "darwin" "amd64"
#build_target "faf-adapter" "linux/arm64" "darwin" "arm64"
#
## Build for windows/amd64 (it's still built in linux container, that's possible)
#build_target "faf-adapter" "linux/amd64" "windows" "amd64"

build_target "faf-adapter" "pioneer" "linux/amd64" "linux" "amd64"
build_target "faf-launcher-emulator" "launcher-emu" "linux/amd64" "linux" "amd64"

echo "Building finished."
