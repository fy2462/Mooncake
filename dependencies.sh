#!/bin/bash
# Copyright 2024 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Color definitions
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Configuration
REPO_ROOT=`pwd`
GITHUB_PROXY=${GITHUB_PROXY:-"https://github.com"}
GOVER=1.25.9
SPDK_REPOSITORY=openebs/spdk
SPDK_RS_REPOSITORY=openebs/spdk-rs
SPDK_COMMIT=cc090cd2b64775545eb38022bb0ec8f37f4741a6
DPDK_COMMIT=cf36799c473a686fa14fde9af97f917a2125d3d5
SPDK_RS_COMMIT=78d6018af041e80a42e222165b86070bae631821
SPDK_ISAL_CRYPTO_CONFIGURE_SHA256=1e30f91190895a6b4f189035ce7361cf52b74a315d1c20030675f79097d15ba3
INSTALL_USER=${SUDO_USER:-$(id -un)}
INSTALL_HOME=${INSTALL_HOME:-$(getent passwd "$INSTALL_USER" | cut -d: -f6)}
SHARED_BUILD_ROOT=${SHARED_BUILD_ROOT:-"$INSTALL_HOME/workspace/tmp/mooncake"}
SPDK_SOURCE_DIR=${SPDK_SOURCE_DIR:-"$SHARED_BUILD_ROOT/spdk-25.05"}
SPDK_RS_SOURCE_DIR=${SPDK_RS_SOURCE_DIR:-"$SHARED_BUILD_ROOT/spdk-rs-v2.11.0"}
SPDK_STAGING_DIR=${SPDK_STAGING_DIR:-"$SHARED_BUILD_ROOT/spdk-sdk-25.05"}
SPDK_INSTALL_PREFIX=${SPDK_INSTALL_PREFIX:-/usr/local}
SPDK_INSTALL_MANIFEST=${SPDK_INSTALL_MANIFEST:-"$SPDK_INSTALL_PREFIX/share/mooncake/spdk-25.05.manifest"}
OS_RELEASE_FILE=${OS_RELEASE_FILE:-/etc/os-release}

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error messages and exit
print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# Function to check command success
check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

# spdk-rs v2.11.0 intentionally applies one tracked isa-l-crypto fix during
# configure. Accept that exact post-build state on reinstall, but reject every
# other tracked source modification.
is_expected_spdk_build_patch() {
    local spdk_status
    local isal_crypto_status
    local configure_sha256

    spdk_status=$(git -C "$SPDK_SOURCE_DIR" status --porcelain --untracked-files=no)
    [ "$spdk_status" = " m isa-l-crypto" ] || return 1

    isal_crypto_status=$(git -C "$SPDK_SOURCE_DIR/isa-l-crypto" status --porcelain --untracked-files=no)
    [ "$isal_crypto_status" = " M configure.ac" ] || return 1

    configure_sha256=$(sha256sum "$SPDK_SOURCE_DIR/isa-l-crypto/configure.ac" | awk '{print $1}')
    [ "$configure_sha256" = "$SPDK_ISAL_CRYPTO_CONFIGURE_SHA256" ]
}

deploy_spdk_sdk() {
    local staging_dir="$1"
    local manifest_dir
    local manifest_tmp
    local installed_path
    local normalized_path
    local normalized_prefix
    local relative_path
    local source_path
    local physical_staging_dir
    local physical_source_dir
    declare -A owned_paths=()

    assert_safe_spdk_parent() {
        local candidate="$1"
        local parent
        local relative_parent
        local component
        local current="$normalized_prefix"

        parent=$(dirname "$candidate")
        [ ! -L "$normalized_prefix" ] || print_error "SPDK_INSTALL_PREFIX must not be a symlink"
        [ "$parent" = "$normalized_prefix" ] && return
        relative_parent=${parent#"$normalized_prefix/"}
        IFS=/ read -r -a components <<< "$relative_parent"
        for component in "${components[@]}"; do
            current="$current/$component"
            [ ! -L "$current" ] || print_error "Symlink parent is unsafe for SPDK deployment: $current"
        done
    }

    case "$SPDK_INSTALL_PREFIX" in
        /*) ;;
        *) print_error "SPDK_INSTALL_PREFIX must be an absolute path" ;;
    esac
    [ "$SPDK_INSTALL_PREFIX" != "/" ] || print_error "Refusing to install SPDK into /"
    [ -d "$staging_dir/include/spdk" ] || print_error "SPDK staging directory is incomplete: $staging_dir"
    normalized_prefix=$(realpath -ms "$SPDK_INSTALL_PREFIX")
    [ "$normalized_prefix" = "$SPDK_INSTALL_PREFIX" ] || print_error "SPDK_INSTALL_PREFIX must be normalized"
    normalized_path=$(realpath -ms "$SPDK_INSTALL_MANIFEST")
    case "$normalized_path" in
        "$normalized_prefix"/*) ;;
        *) print_error "SPDK_INSTALL_MANIFEST must be inside SPDK_INSTALL_PREFIX" ;;
    esac
    [ "$normalized_path" = "$SPDK_INSTALL_MANIFEST" ] || print_error "SPDK_INSTALL_MANIFEST must be normalized"
    [ ! -L "$SPDK_INSTALL_MANIFEST" ] || print_error "SPDK_INSTALL_MANIFEST must not be a symlink"
    assert_safe_spdk_parent "$SPDK_INSTALL_MANIFEST"

    if [ -f "$SPDK_INSTALL_MANIFEST" ]; then
        while IFS= read -r installed_path; do
            [ -n "$installed_path" ] || continue
            normalized_path=$(realpath -ms "$installed_path")
            case "$normalized_path" in
                "$normalized_prefix"/*) ;;
                *) print_error "Unsafe path in SPDK install manifest: $installed_path" ;;
            esac
            [ "$normalized_path" = "$installed_path" ] || print_error "Non-normalized path in SPDK install manifest: $installed_path"
            assert_safe_spdk_parent "$installed_path"
            owned_paths["$installed_path"]=1
        done < "$SPDK_INSTALL_MANIFEST"
    fi

    while IFS= read -r -d '' relative_path; do
        relative_path=${relative_path#./}
        installed_path="$SPDK_INSTALL_PREFIX/$relative_path"
        assert_safe_spdk_parent "$installed_path"
        if { [ -e "$installed_path" ] || [ -L "$installed_path" ]; } && [ -z "${owned_paths["$installed_path"]+owned}" ]; then
            print_error "Refusing to overwrite unmanaged path: $installed_path"
        fi
    done < <(cd "$staging_dir" && find . \( -type f -o -type l \) -print0)

    # Publish a union ownership journal before mutating the installation. If a
    # later copy or metadata rewrite fails, the next run can safely remove both
    # old files and any partially deployed new files instead of treating them
    # as unmanaged collisions.
    manifest_dir=$(dirname "$SPDK_INSTALL_MANIFEST")
    mkdir -p "$manifest_dir"
    manifest_tmp=$(mktemp "$manifest_dir/.spdk-25.05.manifest.XXXXXX")
    {
        [ ! -f "$SPDK_INSTALL_MANIFEST" ] || cat "$SPDK_INSTALL_MANIFEST"
        (
            cd "$staging_dir" || exit 1
            find . \( -type f -o -type l \) -print | sed "s|^\.|$SPDK_INSTALL_PREFIX|"
        )
    } | LC_ALL=C sort -u > "$manifest_tmp"
    check_success "Failed to create the SPDK deployment journal"
    chmod 0644 "$manifest_tmp"
    mv -f "$manifest_tmp" "$SPDK_INSTALL_MANIFEST"
    check_success "Failed to publish the SPDK deployment journal"

    if [ -f "$SPDK_INSTALL_MANIFEST" ]; then
        while IFS= read -r installed_path; do
            [ -n "$installed_path" ] || continue
            assert_safe_spdk_parent "$installed_path"
            if [ -f "$installed_path" ] || [ -L "$installed_path" ]; then
                unlink "$installed_path"
                check_success "Failed to remove prior SPDK SDK file $installed_path"
            fi
        done < "$SPDK_INSTALL_MANIFEST"
    fi

    while IFS= read -r -d '' source_path; do
        relative_path=${source_path#"$staging_dir/"}
        installed_path="$SPDK_INSTALL_PREFIX/$relative_path"
        assert_safe_spdk_parent "$installed_path"
        mkdir -p "$(dirname "$installed_path")"
        cp -a "$source_path" "$installed_path"
        check_success "Failed to deploy SPDK SDK file $installed_path"
    done < <(find "$staging_dir" \( -type f -o -type l \) -print0)

    physical_staging_dir=$(realpath -m "$staging_dir")
    physical_source_dir=$(realpath -m "$SPDK_SOURCE_DIR")
    while IFS= read -r -d '' installed_path; do
        installed_path="$SPDK_INSTALL_PREFIX/${installed_path#"$staging_dir/"}"
        sed -i \
            -e "s|$staging_dir|$SPDK_INSTALL_PREFIX|g" \
            -e "s|$physical_staging_dir|$SPDK_INSTALL_PREFIX|g" \
            -e "s|$SPDK_SOURCE_DIR/build/include|$SPDK_INSTALL_PREFIX/include|g" \
            -e "s|$physical_source_dir/build/include|$SPDK_INSTALL_PREFIX/include|g" \
            "$installed_path"
        check_success "Failed to rewrite pkg-config prefix in $installed_path"
    done < <(find "$staging_dir/lib/pkgconfig" -type f -name '*.pc' -print0)
    if grep -R -F -e "$staging_dir" -e "$physical_staging_dir" -e "$SPDK_SOURCE_DIR" -e "$physical_source_dir" "$SPDK_INSTALL_PREFIX/lib/pkgconfig"; then
        print_error "Installed SPDK pkg-config metadata still references shared build paths"
    fi

    manifest_tmp=$(mktemp "$manifest_dir/.spdk-25.05.manifest.XXXXXX")
    (
        cd "$staging_dir" || exit 1
        find . \( -type f -o -type l \) -print | LC_ALL=C sort | sed "s|^\.|$SPDK_INSTALL_PREFIX|"
    ) > "$manifest_tmp"
    check_success "Failed to create the SPDK install manifest"
    chmod 0644 "$manifest_tmp"
    mv -f "$manifest_tmp" "$SPDK_INSTALL_MANIFEST"
    check_success "Failed to install the SPDK install manifest"
}

# Allow installer deployment behavior to be exercised in an isolated prefix
# without running package installation or source builds.
if [ "${MOONCAKE_DEPENDENCIES_LIBRARY_ONLY:-0}" = 1 ]; then
    return 0
fi

read_os_release_value() {
    local key="$1"
    awk -F= -v key="$key" '
        $1 == key {
            value = $0
            sub(/^[^=]*=/, "", value)
            gsub(/^"|"$/, "", value)
            print value
            exit
        }
    ' "$OS_RELEASE_FILE"
}

# Function to detect OS
detect_os() {
    if [ -f "$OS_RELEASE_FILE" ]; then
        ID=$(read_os_release_value ID)
        VERSION_ID=$(read_os_release_value VERSION_ID)
        OS=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
        OS_VERSION=$VERSION_ID
    elif [ -f /etc/redhat-release ]; then
        OS="centos"
    else
        print_error "Cannot detect OS. Supported OS: Ubuntu, Debian, CentOS, RHEL, Rocky, AlmaLinux, EulerOS, and openEuler."
    fi

    echo -e "${GREEN}Detected OS: $OS ${OS_VERSION:-unknown}${NC}"
}

if [ $(id -u) -ne 0 ]; then
	print_error "Require root permission, try sudo ./dependencies.sh"
fi

# Parse command line arguments
SKIP_CONFIRM=false
INSTALL_SPDK=false
for arg in "$@"; do
    case $arg in
        -y|--yes)
            SKIP_CONFIRM=true
            ;;
        --with-spdk)
            INSTALL_SPDK=true
            ;;
        -h|--help)
            echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
            echo -e "Usage: ./dependencies.sh [OPTIONS]"
            echo -e "\nOptions:"
            echo -e "  -y, --yes       Skip confirmation and install all dependencies"
            echo -e "  --with-spdk     Install SPDK for NVMe-oF support"
            echo -e "  -h, --help      Show this help message and exit"
            exit 0
            ;;
    esac
done

# Print welcome message
echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
echo -e "This script will install all required dependencies for Mooncake."
echo -e "The following components will be installed:"
echo -e "  - System packages (build tools, libraries)"
echo -e "  - Git submodules (including pybind11 and yalantinglibs)"
echo -e "  - Go $GOVER"
if [ "$INSTALL_SPDK" = true ]; then
    echo -e "  - SPDK (for NVMe-oF support)"
fi
echo

# Ask for confirmation unless -y flag is used
if [ "$SKIP_CONFIRM" = false ]; then
    read -p "Do you want to continue? [Y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! $REPLY = "" ]]; then
        echo -e "${YELLOW}Installation cancelled.${NC}"
        exit 0
    fi
fi

# Detect OS
detect_os

# Update package lists
print_section "Updating package lists"
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    apt-get update
    check_success "Failed to update package lists"
elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ]; then
    yum install -y dnf-plugins-core epel-release || true
    yum config-manager --set-enabled powertools || yum config-manager --set-enabled crb || true
    yum clean all
    yum makecache
    check_success "Failed to update package lists"
else
    print_error "Unsupported OS: $OS"
fi

# Install system packages
print_section "Installing system packages"
echo -e "${YELLOW}This may take a few minutes...${NC}"

if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    SYSTEM_PACKAGES="build-essential \
                     cmake \
                     ninja-build \
                     git \
                     wget \
                     unzip \
                     libibverbs-dev \
                     libgoogle-glog-dev \
                     libjsoncpp-dev \
                     libunwind-dev \
                     libnuma-dev \
                     libpython3-dev \
                     libboost-all-dev \
                     libssl-dev \
                     libgrpc-dev \
                     libgrpc++-dev \
                     libprotobuf-dev \
                     libyaml-cpp-dev \
                     protobuf-compiler-grpc \
                     libcurl4-openssl-dev \
                     libhiredis-dev \
                     liburing-dev \
                     libjemalloc-dev \
                     libmsgpack-dev \
                     libmsgpack-cxx-dev \
                     libzmq3-dev \
                     libzstd-dev \
                     libjitterentropy3-dev \
                     libasio-dev \
                     libxxhash-dev \
                     pkg-config \
                     patchelf \
                     libc6-dev \
                     libc-bin"

    apt-get install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"

elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ]; then
    SYSTEM_PACKAGES="@development \
                     cmake \
                     ninja-build \
                     git \
                     wget \
                     rdma-core-devel \
                     glog-devel \
                     gflags-devel \
                     jsoncpp-devel \
                     libunwind-devel \
                     numactl-devel \
                     python3-devel \
                     boost1.78-devel \
                     openssl-devel \
                     protobuf-devel \
                     yaml-cpp-devel \
                     libcurl-devel \
                     hiredis-devel \
                     liburing-devel \
                     jemalloc-devel \
                     msgpack-devel \
                     libzstd-devel \
                     pkgconf-pkg-config \
                     elfutils-libelf-devel \
                     patchelf  \
                     xxhash-devel \
                     libbsd-devel"

    yum install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"
else
    print_error "Unsupported OS: $OS"
fi

print_success "System packages installed successfully"

# Initialize and update git submodules
print_section "Initializing Git Submodules"

# Check if .gitmodules exists
if [ -f "${REPO_ROOT}/.gitmodules" ]; then
    echo "Enter repository root: ${REPO_ROOT}"
    cd "${REPO_ROOT}"
    check_success "Failed to change to repository root directory"

    echo "Initializing git submodules..."
    git submodule sync --recursive
    check_success "Failed to sync git submodules"
    git submodule update --init --recursive
    check_success "Failed to initialize git submodules"

    print_success "Git submodules initialized and updated successfully"
else
    echo -e "${YELLOW}No .gitmodules file found. Skipping...${NC}"
    exit 1
fi

# Build and install yalantinglibs from submodule
print_section "Installing yalantinglibs"
cd "${REPO_ROOT}/extern/yalantinglibs"
check_success "Failed to change to yalantinglibs submodule directory"

mkdir -p build
check_success "Failed to create build directory"
cd build
check_success "Failed to change to build directory"

echo "Configuring yalantinglibs..."
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF
check_success "Failed to configure yalantinglibs"

echo "Building yalantinglibs (using $(nproc) cores)..."
cmake --build . -j$(nproc)
check_success "Failed to build yalantinglibs"

echo "Installing yalantinglibs..."
cmake --install .
check_success "Failed to install yalantinglibs"

print_success "yalantinglibs installed successfully"
cd "${REPO_ROOT}"

print_section "Verifying essential build tools"

# Verify getconf and ldd (required for glibc version detection in build_wheel.sh)
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    if ! command -v getconf >/dev/null 2>&1; then
        print_error "getconf not found after installing system packages. This should not happen."
    fi
    if ! command -v ldd >/dev/null 2>&1; then
        print_error "ldd not found after installing system packages. This should not happen."
    fi
    print_success "getconf found: $(getconf --version 2>&1 | head -1)"
    print_success "ldd found: $(ldd --version 2>&1 | head -1)"
fi

print_section "Installing Go $GOVER"

USED_CN_MIRROR=false

install_go() {
    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        ARCH="arm64"
    elif [ "$ARCH" = "x86_64" ]; then
        ARCH="amd64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    GO_TARBALL="go$GOVER.linux-$ARCH.tar.gz"

    # Try multiple download mirrors with fallback
    GO_DOWNLOAD_URLS=(
        "https://go.dev/dl/${GO_TARBALL}"
        "https://golang.google.cn/dl/${GO_TARBALL}"
        "https://mirrors.aliyun.com/golang/${GO_TARBALL}"
    )

    DOWNLOAD_SUCCESS=false
    for url in "${GO_DOWNLOAD_URLS[@]}"; do
        echo "Downloading Go $GOVER from ${url}..."
        if wget -q --show-progress --timeout=30 --tries=2 -O "${GO_TARBALL}" "${url}"; then
            DOWNLOAD_SUCCESS=true
            if [[ "$url" != "https://go.dev/dl/${GO_TARBALL}" ]]; then
                USED_CN_MIRROR=true
            fi
            print_success "Downloaded Go $GOVER from ${url}"
            break
        else
            echo -e "${YELLOW}Failed to download from ${url}, trying next mirror...${NC}"
            rm -f "${GO_TARBALL}"
        fi
    done

    if [ "$DOWNLOAD_SUCCESS" = false ]; then
        print_error "Failed to download Go $GOVER from all mirrors"
    fi

    echo "Installing Go $GOVER..."
    tar -C /usr/local -xzf "${GO_TARBALL}"
    check_success "Failed to install Go $GOVER"

    rm -f "${GO_TARBALL}"
    check_success "Failed to clean up Go installation file"

    print_success "Go $GOVER installed successfully"
}

if command -v go &> /dev/null; then
    GO_VERSION=$(go version | awk '{print $3}')
    if [[ "$GO_VERSION" == "go$GOVER" ]]; then
        echo -e "${YELLOW}Go $GOVER is already installed. Skipping...${NC}"
    else
        echo -e "${YELLOW}Found Go $GO_VERSION. Will install Go $GOVER...${NC}"
        install_go
    fi
else
    install_go
fi

# Add Go to PATH if not already there
if ! grep -q "export PATH=\$PATH:/usr/local/go/bin" ~/.bashrc; then
    echo -e "${YELLOW}Adding Go to your PATH in ~/.bashrc${NC}"
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    echo -e "${YELLOW}Please run 'source ~/.bashrc' or start a new terminal to use Go${NC}"
fi

# Set GOPROXY only if Go download fell back to a CN mirror
if [ "$USED_CN_MIRROR" = true ] && [ -z "$GOPROXY" ]; then
    export GOPROXY=https://goproxy.cn,https://goproxy.io,direct
    echo -e "${YELLOW}Detected restricted network (Go was downloaded from a CN mirror).${NC}"
    echo -e "${YELLOW}GOPROXY set to: ${GOPROXY}${NC}"
    if ! grep -q "export GOPROXY=" ~/.bashrc; then
        echo 'export GOPROXY=https://goproxy.cn,https://goproxy.io,direct' >> ~/.bashrc
        echo -e "${YELLOW}GOPROXY added to ~/.bashrc for future sessions${NC}"
    fi
elif [ -n "$GOPROXY" ]; then
    echo -e "${GREEN}GOPROXY already set to: ${GOPROXY}${NC}"
fi

# Install SPDK if requested
if [ "$INSTALL_SPDK" = true ]; then
    print_section "Installing SPDK"

    mkdir -p "$SHARED_BUILD_ROOT"
    check_success "Failed to create the shared SPDK build directory"

    if [ -e "$SPDK_SOURCE_DIR" ] && [ ! -d "$SPDK_SOURCE_DIR/.git" ]; then
        print_error "$SPDK_SOURCE_DIR exists but is not a Git checkout"
    fi

    if [ ! -d "$SPDK_SOURCE_DIR/.git" ]; then
        echo "Cloning SPDK from ${GITHUB_PROXY}/${SPDK_REPOSITORY}.git..."
        git clone "${GITHUB_PROXY}/${SPDK_REPOSITORY}.git" "$SPDK_SOURCE_DIR"
        check_success "Failed to clone SPDK"
    elif [ -n "$(git -C "$SPDK_SOURCE_DIR" status --porcelain --untracked-files=no)" ]; then
        if ! is_expected_spdk_build_patch; then
            print_error "$SPDK_SOURCE_DIR has modified tracked files; preserve or revert them before reinstalling"
        fi
        echo -e "${YELLOW}Keeping the pinned spdk-rs isa-l-crypto build patch.${NC}"
    fi

    echo "Checking out OpenEBS SPDK commit $SPDK_COMMIT..."
    git -C "$SPDK_SOURCE_DIR" fetch origin "$SPDK_COMMIT"
    check_success "Failed to fetch SPDK commit $SPDK_COMMIT"
    git -C "$SPDK_SOURCE_DIR" checkout --detach "$SPDK_COMMIT"
    check_success "Failed to checkout SPDK commit $SPDK_COMMIT"

    echo "Initializing SPDK submodules..."
    git -C "$SPDK_SOURCE_DIR" submodule update --init --recursive
    check_success "Failed to initialize SPDK submodules"
    test "$(git -C "$SPDK_SOURCE_DIR/dpdk" rev-parse HEAD)" = "$DPDK_COMMIT"
    check_success "SPDK checkout contains an unexpected DPDK revision"

    if [ -e "$SPDK_RS_SOURCE_DIR" ] && [ ! -d "$SPDK_RS_SOURCE_DIR/.git" ]; then
        print_error "$SPDK_RS_SOURCE_DIR exists but is not a Git checkout"
    fi

    if [ ! -d "$SPDK_RS_SOURCE_DIR/.git" ]; then
        echo "Cloning spdk-rs from ${GITHUB_PROXY}/${SPDK_RS_REPOSITORY}.git..."
        git clone "${GITHUB_PROXY}/${SPDK_RS_REPOSITORY}.git" "$SPDK_RS_SOURCE_DIR"
        check_success "Failed to clone spdk-rs"
    elif [ -n "$(git -C "$SPDK_RS_SOURCE_DIR" status --porcelain --untracked-files=no)" ]; then
        print_error "$SPDK_RS_SOURCE_DIR has modified tracked files; preserve or revert them before reinstalling"
    fi

    git -C "$SPDK_RS_SOURCE_DIR" fetch origin "$SPDK_RS_COMMIT"
    check_success "Failed to fetch spdk-rs commit $SPDK_RS_COMMIT"
    git -C "$SPDK_RS_SOURCE_DIR" checkout --detach "$SPDK_RS_COMMIT"
    check_success "Failed to checkout spdk-rs commit $SPDK_RS_COMMIT"

    # Install SPDK dependencies
    echo "Installing SPDK dependencies..."
    "$SPDK_SOURCE_DIR/scripts/pkgdep.sh"
    check_success "Failed to install SPDK dependencies"

    case "$(uname -m)" in
        x86_64) SPDK_TARGET=x86_64-unknown-linux-gnu ;;
        aarch64) SPDK_TARGET=aarch64-unknown-linux-gnu ;;
        *) print_error "Unsupported SPDK architecture: $(uname -m)" ;;
    esac
    SPDK_BUILD_SCRIPT="$SPDK_RS_SOURCE_DIR/build_scripts/build_spdk.sh"
    SPDK_BUILD_ARGS=(-b release -t "$SPDK_TARGET" -s "$SPDK_SOURCE_DIR" --without-fio --no-log)

    echo "Configuring SPDK with the pinned spdk-rs build helper..."
    "$SPDK_BUILD_SCRIPT" "${SPDK_BUILD_ARGS[@]}" configure
    check_success "Failed to configure SPDK"

    echo "Building SPDK (using $(nproc) cores)..."
    "$SPDK_BUILD_SCRIPT" "${SPDK_BUILD_ARGS[@]}" make
    check_success "Failed to build SPDK"

    echo "Staging the SPDK SDK in $SPDK_STAGING_DIR..."
    "$SPDK_BUILD_SCRIPT" "${SPDK_BUILD_ARGS[@]}" install "$SPDK_STAGING_DIR"
    check_success "Failed to install the SPDK SDK"

    echo "Deploying the SPDK SDK to $SPDK_INSTALL_PREFIX..."
    deploy_spdk_sdk "$SPDK_STAGING_DIR"

    print_success "SPDK installed successfully"
    export SPDK_ROOT_DIR="$SPDK_INSTALL_PREFIX"
    export PKG_CONFIG_PATH="$SPDK_INSTALL_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    echo -e "${YELLOW}For Rust SPDK builds, run: export SPDK_ROOT_DIR=$SPDK_INSTALL_PREFIX${NC}"
    echo -e "${YELLOW}export PKG_CONFIG_PATH=$SPDK_INSTALL_PREFIX/lib/pkgconfig\${PKG_CONFIG_PATH:+:\$PKG_CONFIG_PATH}${NC}"
    cd "${REPO_ROOT}"
fi

# Return to the repository root
cd "${REPO_ROOT}"

# Print summary
print_section "Installation Complete"
echo -e "${GREEN}All dependencies have been successfully installed!${NC}"
echo -e "The following components were installed:"
echo -e "  ${GREEN}✓${NC} System packages"
echo -e "  ${GREEN}✓${NC} yalantinglibs"
echo -e "  ${GREEN}✓${NC} Git submodules"
echo -e "  ${GREEN}✓${NC} Go $GOVER"
if [ "$INSTALL_SPDK" = true ]; then
    echo -e "  ${GREEN}✓${NC} OpenEBS SPDK ($SPDK_COMMIT)"
fi
echo
echo -e "You can now build and run Mooncake."
echo -e "${YELLOW}Note: You may need to restart your terminal or run 'source ~/.bashrc' to use Go.${NC}"

if [ "$INSTALL_SPDK" = true ]; then
    echo -e "${YELLOW}Note: SPDK requires hugepages and RDMA configuration. Please refer to SPDK documentation for setup.${NC}"
fi
