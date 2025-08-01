#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to compile Apache Doris
# Usage:
#    sh build.sh --help
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"
if [[ -z "${DORIS_THIRDPARTY}" ]]; then
    export DORIS_THIRDPARTY="${DORIS_HOME}/thirdparty"
fi
export TP_INCLUDE_DIR="${DORIS_THIRDPARTY}/installed/include"
export TP_LIB_DIR="${DORIS_THIRDPARTY}/installed/lib"

TARGET_SYSTEM="$(uname -s)"
TARGET_ARCH="$(uname -m)"
echo "Target system: ${TARGET_SYSTEM}; Target arch: ${TARGET_ARCH}"

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]                build all components
     --fe                       build Frontend. Default ON.
     --be                       build Backend. Default ON.
     --meta-tool                build Backend meta tool. Default OFF.
     --file-cache-microbench    build Backend file cache microbench tool. Default OFF.
     --cloud                    build Cloud. Default OFF.
     --index-tool               build Backend inverted index tool. Default OFF.
     --benchmark                build Google Benchmark. Default OFF.
     --task-executor-simulator  build Backend task executor simulator. Default OFF.
     --broker                   build Broker. Default ON.
     --hive-udf                 build Hive UDF library for Ingestion Load. Default ON.
     --be-java-extensions       build Backend java extensions. Default ON.
     --be-extension-ignore      build be-java-extensions package, choose which modules to ignore. Multiple modules separated by commas.
     --clean                    clean and build target
     --output                   specify the output directory
     -j                         build Backend parallel

  Environment variables:
    USE_AVX2                    If the CPU does not support AVX2 instruction set, please set USE_AVX2=0. Default is ON.
    ARM_MARCH                   Specify the ARM architecture instruction set. Default is armv8-a+crc.
    STRIP_DEBUG_INFO            If set STRIP_DEBUG_INFO=ON, the debug information in the compiled binaries will be stored separately in the 'be/lib/debug_info' directory. Default is OFF.
    DISABLE_BE_JAVA_EXTENSIONS  If set DISABLE_BE_JAVA_EXTENSIONS=ON, we will do not build binary with java-udf,hadoop-hudi-scanner,jdbc-scanner and so on Default is OFF.
    DISABLE_JAVA_CHECK_STYLE    If set DISABLE_JAVA_CHECK_STYLE=ON, it will skip style check of java code in FE.
    DISABLE_BUILD_AZURE         If set DISABLE_BUILD_AZURE=ON, it will not build azure into BE.

  Eg.
    $0                                      build all
    $0 --be                                 build Backend
    $0 --meta-tool                          build Backend meta tool
    $0 --file-cache-microbench              build Backend file cache microbench tool
    $0 --cloud                              build Cloud
    $0 --index-tool                         build Backend inverted index tool
    $0 --benchmark                          build Google Benchmark of Backend
    $0 --fe --clean                         clean and build Frontend.
    $0 --fe --be --clean                    clean and build Frontend and Backend
    $0 --task-executor-simulator            build task executor simulator
    $0 --broker                             build Broker
    $0 --be --fe                            build Backend, Frontend, and Java UDF library
    $0 --be --coverage                      build Backend with coverage enabled
    $0 --be --output PATH                   build Backend, the result will be output to PATH(relative paths are available)
    $0 --be-extension-ignore avro-scanner   build be-java-extensions, choose which modules to ignore. Multiple modules separated by commas, like --be-extension-ignore avro-scanner,hadoop-hudi-scanner

    USE_AVX2=0 $0 --be                      build Backend and not using AVX2 instruction.
    USE_AVX2=0 STRIP_DEBUG_INFO=ON $0       build all and not using AVX2 instruction, and strip the debug info for Backend
    ARM_MARCH=armv8-a+crc+simd $0 --be      build Backend with specified ARM architecture instruction set
  "
    exit 1
}

clean_gensrc() {
    pushd "${DORIS_HOME}/gensrc"
    make clean
    rm -rf "${DORIS_HOME}/gensrc/build"
    rm -rf "${DORIS_HOME}/fe/fe-common/target"
    rm -rf "${DORIS_HOME}/fe/fe-core/target"
    popd
}

clean_be() {
    pushd "${DORIS_HOME}"

    # "build.sh --clean" just cleans and exits, however CMAKE_BUILD_DIR is set
    # while building be.
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"

    rm -rf "${CMAKE_BUILD_DIR}"
    rm -rf "${DORIS_HOME}/be/output"
    popd
}

clean_fe() {
    pushd "${DORIS_HOME}/fe"
    "${MVN_CMD}" clean
    popd
}

# Copy the common files like licenses, notice.txt to output folder
function copy_common_files() {
    cp -r -p "${DORIS_HOME}/NOTICE.txt" "$1/"
    cp -r -p "${DORIS_HOME}/dist/LICENSE-dist.txt" "$1/"
    cp -r -p "${DORIS_HOME}/dist/licenses" "$1/"
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'fe' \
    -l 'be' \
    -l 'cloud' \
    -l 'broker' \
    -l 'meta-tool' \
    -l 'file-cache-microbench' \
    -l 'index-tool' \
    -l 'benchmark' \
    -l 'task-executor-simulator' \
    -l 'spark-dpp' \
    -l 'hive-udf' \
    -l 'be-java-extensions' \
    -l 'be-extension-ignore:' \
    -l 'clean' \
    -l 'coverage' \
    -l 'help' \
    -l 'output:' \
    -o 'hj:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

PARALLEL="$(($(nproc) / 4 + 1))"
BUILD_FE=0
BUILD_BE=0
BUILD_CLOUD=0
BUILD_BROKER=0
BUILD_META_TOOL='OFF'
BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
BUILD_INDEX_TOOL='OFF'
BUILD_BENCHMARK='OFF'
BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
BUILD_BE_JAVA_EXTENSIONS=0
BUILD_HIVE_UDF=0
CLEAN=0
HELP=0
PARAMETER_COUNT="$#"
PARAMETER_FLAG=0
DENABLE_CLANG_COVERAGE='OFF'
BUILD_AZURE='ON'
BUILD_UI=1
if [[ "$#" == 1 ]]; then
    # default
    BUILD_FE=1
    BUILD_BE=1
    BUILD_CLOUD=1

    BUILD_BROKER=1
    BUILD_META_TOOL='OFF'
    BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
    BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
    BUILD_INDEX_TOOL='OFF'
    BUILD_BENCHMARK='OFF'
    BUILD_HIVE_UDF=1
    BUILD_BE_JAVA_EXTENSIONS=1
    CLEAN=0
else
    while true; do
        case "$1" in
        --fe)
            BUILD_FE=1
            BUILD_HIVE_UDF=1
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --be)
            BUILD_BE=1
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --cloud)
            BUILD_CLOUD=1
            shift
            ;;
        --broker)
            BUILD_BROKER=1
            shift
            ;;
        --meta-tool)
            BUILD_META_TOOL='ON'
            shift
            ;;
        --file-cache-microbench)
            BUILD_FILE_CACHE_MICROBENCH_TOOL='ON'
            shift
            ;;
        --index-tool)
            BUILD_INDEX_TOOL='ON'
            shift
            ;;
        --benchmark)
            BUILD_BENCHMARK='ON'
            BUILD_BE=1 # go into BE cmake building, but benchmark instead of doris_be
            shift
            ;;
        --task-executor-simulator)
            BUILD_TASK_EXECUTOR_SIMULATOR='ON'
            BUILD_BE=1
            shift
            ;;
        --spark-dpp)
            BUILD_SPARK_DPP=1
            shift
            ;;
        --hive-udf)
            BUILD_HIVE_UDF=1
            shift
            ;;
        --be-java-extensions)
            BUILD_BE_JAVA_EXTENSIONS=1
            shift
            ;;
        --clean)
            CLEAN=1
            shift
            ;;
        --coverage)
            DENABLE_CLANG_COVERAGE='ON'
            shift
            ;;
        -h)
            HELP=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        -j)
            PARALLEL="$2"
            PARAMETER_FLAG=1
            shift 2
            ;;
        --output)
            DORIS_OUTPUT="$2"
            shift 2
            ;;
        --be-extension-ignore)
            BE_EXTENSION_IGNORE="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
    #only ./build.sh -j xx then build all
    if [[ "${PARAMETER_COUNT}" -eq 3 ]] && [[ "${PARAMETER_FLAG}" -eq 1 ]]; then
        BUILD_FE=1
        BUILD_BE=1
        BUILD_CLOUD=1
        BUILD_BROKER=1
        BUILD_META_TOOL='ON'
        BUILD_FILE_CACHE_MICROBENCH_TOOL='OFF'
        BUILD_INDEX_TOOL='ON'
	BUILD_TASK_EXECUTOR_SIMULATOR='OFF'
        BUILD_HIVE_UDF=1
        BUILD_BE_JAVA_EXTENSIONS=1
        CLEAN=0
    fi
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi
# build thirdparty libraries if necessary. check last thirdparty lib installation
if [[ "${TARGET_SYSTEM}" == 'Darwin' ]]; then
    LAST_THIRDPARTY_LIB='libbrotlienc.a'
else
    LAST_THIRDPARTY_LIB='hadoop_hdfs/native/libhdfs.a'
fi
if [[ ! -f "${DORIS_THIRDPARTY}/installed/lib/${LAST_THIRDPARTY_LIB}" ]]; then
    echo "Thirdparty libraries need to be build ..."
    # need remove all installed pkgs because some lib like lz4 will throw error if its lib alreay exists
    rm -rf "${DORIS_THIRDPARTY}/installed"

    if [[ "${CLEAN}" -eq 0 ]]; then
        "${DORIS_THIRDPARTY}/build-thirdparty.sh" -j "${PARALLEL}"
    else
        "${DORIS_THIRDPARTY}/build-thirdparty.sh" -j "${PARALLEL}" --clean
    fi
fi

update_submodule() {
    local submodule_path=$1
    local submodule_name=$2
    local archive_url=$3

    set +e
    cd "${DORIS_HOME}"
    echo "Update ${submodule_name} submodule ..."
    git submodule update --init --recursive "${submodule_path}"
    exit_code=$?
    if [[ "${exit_code}" -eq 0 ]]; then
        cd "${submodule_path}"
        submodule_commit_id=$(git rev-parse HEAD)
        cd -
        expect_submodule_commit_id=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')
        echo "Current commit ID of ${submodule_name} submodule: ${submodule_commit_id}, expected is ${expect_submodule_commit_id}"
    fi
    set -e
    if [[ "${exit_code}" -ne 0 ]]; then
        set +e
        # try to get submodule's current commit
        submodule_commit=$(git ls-tree HEAD "${submodule_path}" | awk '{print $3}')
        exit_code=$?
        if [[ "${exit_code}" = "0" ]]; then
            commit_specific_url=$(echo "${archive_url}" | sed "s/refs\/heads/${submodule_commit}/")
        else
            commit_specific_url="${archive_url}"
        fi
        set -e
        echo "Update ${submodule_name} submodule failed, start to download and extract ${commit_specific_url}"

        mkdir -p "${DORIS_HOME}/${submodule_path}"
        curl -L "${commit_specific_url}" | tar -xz -C "${DORIS_HOME}/${submodule_path}" --strip-components=1
    fi
}

if [[ "${CLEAN}" -eq 1 && "${BUILD_BE}" -eq 0 && "${BUILD_FE}" -eq 0 && ${BUILD_CLOUD} -eq 0 ]]; then
    clean_gensrc
    clean_be
    clean_fe
    exit 0
fi

if [[ -z "${GLIBC_COMPATIBILITY}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        GLIBC_COMPATIBILITY='ON'
    else
        GLIBC_COMPATIBILITY='OFF'
    fi
fi
if [[ -z "${USE_AVX2}" ]]; then
    USE_AVX2='ON'
fi
if [[ -z "${ARM_MARCH}" ]]; then
    ARM_MARCH='armv8-a+crc'
fi
if [[ -z "${USE_LIBCPP}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_LIBCPP='OFF'
    else
        USE_LIBCPP='ON'
    fi
fi
if [[ -z "${STRIP_DEBUG_INFO}" ]]; then
    STRIP_DEBUG_INFO='OFF'
fi
BUILD_TYPE_LOWWER=$(echo "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')
if [[ "${BUILD_TYPE_LOWWER}" == "asan" ]]; then
    USE_JEMALLOC='OFF'
elif [[ -z "${USE_JEMALLOC}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_JEMALLOC='ON'
    else
        USE_JEMALLOC='OFF'
    fi
fi
if [[ -f "${TP_INCLUDE_DIR}/jemalloc/jemalloc_doris_with_prefix.h" ]]; then
    # compatible with old thirdparty
    rm -rf "${TP_INCLUDE_DIR}/jemalloc/jemalloc.h"
    rm -rf "${TP_LIB_DIR}/libjemalloc_doris.a"
    rm -rf "${TP_LIB_DIR}/libjemalloc_doris_pic.a"
    rm -rf "${TP_INCLUDE_DIR}/rocksdb"
    rm -rf "${TP_LIB_DIR}/librocksdb.a"

    mv "${TP_INCLUDE_DIR}/jemalloc/jemalloc_doris_with_prefix.h" "${TP_INCLUDE_DIR}/jemalloc/jemalloc.h"
    mv "${TP_LIB_DIR}/libjemalloc_doris_with_prefix.a" "${TP_LIB_DIR}/libjemalloc_doris.a"
    mv "${TP_LIB_DIR}/libjemalloc_doris_with_prefix_pic.a" "${TP_LIB_DIR}/libjemalloc_doris_pic.a"
    mv "${TP_LIB_DIR}/librocksdb_jemalloc_with_prefix.a" "${TP_LIB_DIR}/librocksdb.a"
    mv -f "${TP_INCLUDE_DIR}/rocksdb_jemalloc_with_prefix" "${TP_INCLUDE_DIR}/rocksdb"
fi
if [[ -z "${USE_BTHREAD_SCANNER}" ]]; then
    USE_BTHREAD_SCANNER='OFF'
fi

if [[ -z "${USE_DWARF}" ]]; then
    USE_DWARF='OFF'
fi

if [[ -z "${USE_UNWIND}" ]]; then
    if [[ "${TARGET_SYSTEM}" != 'Darwin' ]]; then
        USE_UNWIND='ON'
    else
        USE_UNWIND='OFF'
    fi
fi

if [[ -z "${DISPLAY_BUILD_TIME}" ]]; then
    DISPLAY_BUILD_TIME='OFF'
fi

if [[ -z "${OUTPUT_BE_BINARY}" ]]; then
    OUTPUT_BE_BINARY=${BUILD_BE}
fi

if [[ -n "${DISABLE_BE_JAVA_EXTENSIONS}" ]]; then
    if [[ "${DISABLE_BE_JAVA_EXTENSIONS}" == "ON" ]]; then
        BUILD_BE_JAVA_EXTENSIONS=0
    else
        BUILD_BE_JAVA_EXTENSIONS=1
    fi
fi

if [[ -n "${DISABLE_BUILD_UI}" ]]; then
    if [[ "${DISABLE_BUILD_UI}" == "ON" ]]; then
        BUILD_UI=0
    fi
fi

if [[ -n "${DISABLE_BUILD_HIVE_UDF}" ]]; then
    if [[ "${DISABLE_BUILD_HIVE_UDF}" == "ON" ]]; then
        BUILD_HIVE_UDF=0
    fi
fi

if [[ -z "${DISABLE_JAVA_CHECK_STYLE}" ]]; then
    DISABLE_JAVA_CHECK_STYLE='OFF'
fi

if [[ -n "${DISABLE_BUILD_AZURE}" || "${TARGET_SYSTEM}" == 'Darwin' ]]; then
    BUILD_AZURE='OFF'
fi

if [[ -z "${ENABLE_INJECTION_POINT}" ]]; then
    ENABLE_INJECTION_POINT='OFF'
fi

if [[ -z "${ENABLE_CACHE_LOCK_DEBUG}" ]]; then
    ENABLE_CACHE_LOCK_DEBUG='ON'
fi

if [[ -z "${BUILD_BENCHMARK}" ]]; then
    BUILD_BENCHMARK='OFF'
fi

if [[ -z "${RECORD_COMPILER_SWITCHES}" ]]; then
    RECORD_COMPILER_SWITCHES='OFF'
fi

if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 && "${TARGET_SYSTEM}" == 'Darwin' ]]; then
    if [[ -z "${JAVA_HOME}" ]]; then
        CAUSE='the environment variable JAVA_HOME is not set'
    else
        LIBJVM="$(find -L "${JAVA_HOME}/" -name 'libjvm.dylib')"
        if [[ -z "${LIBJVM}" ]]; then
            CAUSE="the library libjvm.dylib is missing"
        elif [[ "$(file "${LIBJVM}" | awk '{print $NF}')" != "$(uname -m)" ]]; then
            CAUSE='the architecture which the library libjvm.dylib is built for does not match'
        fi
    fi

    if [[ -n "${CAUSE}" ]]; then
        echo -e "\033[33;1mWARNNING: \033[37;1mSkip building with BE Java extensions due to ${CAUSE}.\033[0m"
        BUILD_BE_JAVA_EXTENSIONS=0
        BUILD_BE_JAVA_EXTENSIONS_FALSE_IN_CONF=1
    fi
fi

echo "Get params:
    BUILD_FE                            -- ${BUILD_FE}
    BUILD_BE                            -- ${BUILD_BE}
    BUILD_CLOUD                         -- ${BUILD_CLOUD}
    BUILD_BROKER                        -- ${BUILD_BROKER}
    BUILD_META_TOOL                     -- ${BUILD_META_TOOL}
    BUILD_FILE_CACHE_MICROBENCH_TOOL    -- ${BUILD_FILE_CACHE_MICROBENCH_TOOL}
    BUILD_INDEX_TOOL                    -- ${BUILD_INDEX_TOOL}
    BUILD_BENCHMARK                     -- ${BUILD_BENCHMARK}
    BUILD_TASK_EXECUTOR_SIMULATOR       -- ${BUILD_TASK_EXECUTOR_SIMULATOR}
    BUILD_BE_JAVA_EXTENSIONS            -- ${BUILD_BE_JAVA_EXTENSIONS}
    BUILD_HIVE_UDF                      -- ${BUILD_HIVE_UDF}
    PARALLEL                            -- ${PARALLEL}
    CLEAN                               -- ${CLEAN}
    GLIBC_COMPATIBILITY                 -- ${GLIBC_COMPATIBILITY}
    USE_AVX2                            -- ${USE_AVX2}
    USE_LIBCPP                          -- ${USE_LIBCPP}
    USE_DWARF                           -- ${USE_DWARF}
    USE_UNWIND                          -- ${USE_UNWIND}
    STRIP_DEBUG_INFO                    -- ${STRIP_DEBUG_INFO}
    USE_JEMALLOC                        -- ${USE_JEMALLOC}
    USE_BTHREAD_SCANNER                 -- ${USE_BTHREAD_SCANNER}
    ENABLE_INJECTION_POINT              -- ${ENABLE_INJECTION_POINT}
    ENABLE_CACHE_LOCK_DEBUG             -- ${ENABLE_CACHE_LOCK_DEBUG}
    DENABLE_CLANG_COVERAGE              -- ${DENABLE_CLANG_COVERAGE}
    DISPLAY_BUILD_TIME                  -- ${DISPLAY_BUILD_TIME}
    ENABLE_PCH                          -- ${ENABLE_PCH}
"

# Clean and build generated code
if [[ "${CLEAN}" -eq 1 ]]; then
    clean_gensrc
fi
"${DORIS_HOME}"/generated-source.sh noclean

# Assesmble FE modules
FE_MODULES=''
modules=("")
if [[ "${BUILD_FE}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("fe-core")
fi
if [[ "${BUILD_HIVE_UDF}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("hive-udf")
fi
if [[ "${BUILD_BE_JAVA_EXTENSIONS}" -eq 1 ]]; then
    modules+=("fe-common")
    modules+=("be-java-extensions/iceberg-metadata-scanner")
    modules+=("be-java-extensions/hadoop-hudi-scanner")
    modules+=("be-java-extensions/java-common")
    modules+=("be-java-extensions/java-udf")
    modules+=("be-java-extensions/jdbc-scanner")
    modules+=("be-java-extensions/paimon-scanner")
    modules+=("be-java-extensions/trino-connector-scanner")
    modules+=("be-java-extensions/max-compute-scanner")
    modules+=("be-java-extensions/avro-scanner")
    modules+=("be-java-extensions/lakesoul-scanner")
    modules+=("be-java-extensions/preload-extensions")

    # If the BE_EXTENSION_IGNORE variable is not empty, remove the modules that need to be ignored from FE_MODULES
    if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
        IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
        for module in "${ignore_modules[@]}"; do
            modules=("${modules[@]/be-java-extensions\/${module}/}")
        done
    fi
fi
FE_MODULES="$(
    IFS=','
    echo "${modules[*]}"
)"

# Clean and build Backend
if [[ "${BUILD_BE}" -eq 1 ]]; then
    update_submodule "contrib/apache-orc" "apache-orc" "https://github.com/apache/doris-thirdparty/archive/refs/heads/orc.tar.gz"
    update_submodule "contrib/clucene" "clucene" "https://github.com/apache/doris-thirdparty/archive/refs/heads/clucene.tar.gz"
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_be
    fi
    MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"

    if [[ -z "${BUILD_FS_BENCHMARK}" ]]; then
        BUILD_FS_BENCHMARK=OFF
    fi

    if [[ -z "${BUILD_TASK_EXECUTOR_SIMULATOR}" ]]; then
        BUILD_TASK_EXECUTOR_SIMULATOR=OFF
    fi

    echo "-- Make program: ${MAKE_PROGRAM}"
    echo "-- Use ccache: ${CMAKE_USE_CCACHE}"
    echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"
    echo "-- Build fs benchmark tool: ${BUILD_FS_BENCHMARK}"
    echo "-- Build task executor simulator: ${BUILD_TASK_EXECUTOR_SIMULATOR}"

    mkdir -p "${CMAKE_BUILD_DIR}"
    cd "${CMAKE_BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_INJECTION_POINT="${ENABLE_INJECTION_POINT}" \
        -DENABLE_CACHE_LOCK_DEBUG="${ENABLE_CACHE_LOCK_DEBUG}" \
        -DMAKE_TEST=OFF \
        -DBUILD_BENCHMARK="${BUILD_BENCHMARK}" \
        -DBUILD_FS_BENCHMARK="${BUILD_FS_BENCHMARK}" \
        -DBUILD_TASK_EXECUTOR_SIMULATOR="${BUILD_TASK_EXECUTOR_SIMULATOR}" \
        ${CMAKE_USE_CCACHE:+${CMAKE_USE_CCACHE}} \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DBUILD_META_TOOL="${BUILD_META_TOOL}" \
        -DBUILD_FILE_CACHE_MICROBENCH_TOOL="${BUILD_FILE_CACHE_MICROBENCH_TOOL}" \
        -DBUILD_INDEX_TOOL="${BUILD_INDEX_TOOL}" \
        -DSTRIP_DEBUG_INFO="${STRIP_DEBUG_INFO}" \
        -DUSE_DWARF="${USE_DWARF}" \
        -DUSE_UNWIND="${USE_UNWIND}" \
        -DDISPLAY_BUILD_TIME="${DISPLAY_BUILD_TIME}" \
        -DENABLE_PCH="${ENABLE_PCH}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DUSE_AVX2="${USE_AVX2}" \
        -DARM_MARCH="${ARM_MARCH}" \
        -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DENABLE_CLANG_COVERAGE="${DENABLE_CLANG_COVERAGE}" \
        -DDORIS_JAVA_HOME="${JAVA_HOME}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        "${DORIS_HOME}/be"

    if [[ "${OUTPUT_BE_BINARY}" -eq 1 ]]; then
        "${BUILD_SYSTEM}" -j "${PARALLEL}"
        "${BUILD_SYSTEM}" install
    fi

    cd "${DORIS_HOME}"
fi

# Clean and build cloud
if [[ "${BUILD_CLOUD}" -eq 1 ]]; then
    if [[ -e "${DORIS_HOME}/gensrc/build/gen_cpp/cloud_version.h" ]]; then
        rm -f "${DORIS_HOME}/gensrc/build/gen_cpp/cloud_version.h"
    fi
    CMAKE_BUILD_TYPE="${BUILD_TYPE:-Release}"
    echo "Build Cloud: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR="${DORIS_HOME}/cloud/build_${CMAKE_BUILD_TYPE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        rm -rf "${CMAKE_BUILD_DIR}"
        echo "clean cloud"
    fi
    MAKE_PROGRAM="$(command -v "${BUILD_SYSTEM}")"
    echo "-- Make program: ${MAKE_PROGRAM}"
    echo "-- Extra cxx flags: ${EXTRA_CXX_FLAGS:-}"
    mkdir -p "${CMAKE_BUILD_DIR}"
    cd "${CMAKE_BUILD_DIR}"
    "${CMAKE_CMD}" -G "${GENERATOR}" \
        -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DENABLE_INJECTION_POINT="${ENABLE_INJECTION_POINT}" \
        -DMAKE_TEST=OFF \
        "${CMAKE_USE_CCACHE}" \
        -DUSE_LIBCPP="${USE_LIBCPP}" \
        -DSTRIP_DEBUG_INFO="${STRIP_DEBUG_INFO}" \
        -DUSE_DWARF="${USE_DWARF}" \
        -DUSE_JEMALLOC="${USE_JEMALLOC}" \
        -DEXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS}" \
        -DBUILD_AZURE="${BUILD_AZURE}" \
        -DBUILD_CHECK_META="${BUILD_CHECK_META:-OFF}" \
        "${DORIS_HOME}/cloud/"
    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
    cd "${DORIS_HOME}"
    echo "Build cloud done"
fi

function build_ui() {
    NPM='npm'
    if ! ${NPM} --version; then
        echo "Error: npm is not found"
        exit 1
    fi
    if [[ -n "${CUSTOM_NPM_REGISTRY}" ]]; then
        "${NPM}" config set registry "${CUSTOM_NPM_REGISTRY}"
        npm_reg="$("${NPM}" get registry)"
        echo "NPM registry: ${npm_reg}"
    fi

    echo "Build Frontend UI"
    ui_dist="${DORIS_HOME}/ui/dist"
    if [[ -n "${CUSTOM_UI_DIST}" ]]; then
        ui_dist="${CUSTOM_UI_DIST}"
    else
        cd "${DORIS_HOME}/ui"
        "${NPM}" cache clean --force
        "${NPM}" install --legacy-peer-deps
        "${NPM}" run build
    fi
    echo "ui dist: ${ui_dist}"
    rm -rf "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    mkdir -p "${DORIS_HOME}/fe/fe-core/src/main/resources/static"
    cp -r "${ui_dist}"/* "${DORIS_HOME}/fe/fe-core/src/main/resources/static"/
}

# FE UI must be built before building FE
if [[ "${BUILD_FE}" -eq 1 ]]; then
    if [[ "${BUILD_UI}" -eq 1 ]]; then
        build_ui
    fi
fi

# Clean and build Frontend
if [[ "${FE_MODULES}" != '' ]]; then
    echo "Build Frontend Modules: ${FE_MODULES}"
    cd "${DORIS_HOME}/fe"
    if [[ "${CLEAN}" -eq 1 ]]; then
        clean_fe
    fi
    if [[ "${DISABLE_JAVA_CHECK_STYLE}" = "ON" ]]; then
        # Allowed user customer set env param USER_SETTINGS_MVN_REPO means settings.xml file path
        if [[ -n ${USER_SETTINGS_MVN_REPO} && -f ${USER_SETTINGS_MVN_REPO} ]]; then
            "${MVN_CMD}" package -pl ${FE_MODULES:+${FE_MODULES}} -Dskip.doc=true -DskipTests -Dcheckstyle.skip=true ${MVN_OPT:+${MVN_OPT}} -gs "${USER_SETTINGS_MVN_REPO}" -T 1C
        else
            "${MVN_CMD}" package -pl ${FE_MODULES:+${FE_MODULES}} -Dskip.doc=true -DskipTests -Dcheckstyle.skip=true ${MVN_OPT:+${MVN_OPT}} -T 1C
        fi
    else
        if [[ -n ${USER_SETTINGS_MVN_REPO} && -f ${USER_SETTINGS_MVN_REPO} ]]; then
            "${MVN_CMD}" package -pl ${FE_MODULES:+${FE_MODULES}} -Dskip.doc=true -DskipTests ${MVN_OPT:+${MVN_OPT}} -gs "${USER_SETTINGS_MVN_REPO}" -T 1C
        else
            "${MVN_CMD}" package -pl ${FE_MODULES:+${FE_MODULES}} -Dskip.doc=true -DskipTests ${MVN_OPT:+${MVN_OPT}} -T 1C
        fi
    fi
    cd "${DORIS_HOME}"
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_OUTPUT:="${DORIS_HOME}/output/"}
echo "OUTPUT DIR=${DORIS_OUTPUT}"
mkdir -p "${DORIS_OUTPUT}"

# Copy Frontend and Backend
if [[ "${BUILD_FE}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/fe/bin" "${DORIS_OUTPUT}/fe/conf" \
        "${DORIS_OUTPUT}/fe/webroot" "${DORIS_OUTPUT}/fe/lib"

    cp -r -p "${DORIS_HOME}/bin"/*_fe.sh "${DORIS_OUTPUT}/fe/bin"/
    cp -r -p "${DORIS_HOME}/conf/fe.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf/ldap.conf" "${DORIS_OUTPUT}/fe/conf"/
    cp -r -p "${DORIS_HOME}/conf/mysql_ssl_default_certificate" "${DORIS_OUTPUT}/fe/"/
    rm -rf "${DORIS_OUTPUT}/fe/lib"/*
    install -d "${DORIS_OUTPUT}/fe/lib/jindofs"
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/lib"/* "${DORIS_OUTPUT}/fe/lib"/
    cp -r -p "${DORIS_HOME}/fe/fe-core/target/doris-fe.jar" "${DORIS_OUTPUT}/fe/lib"/
    #cp -r -p "${DORIS_HOME}/docs/build/help-resource.zip" "${DORIS_OUTPUT}/fe/lib"/

    # copy jindofs jars, only support for Linux x64 or arm
    if [[ "${TARGET_SYSTEM}" == 'Linux' ]] && [[ "${TARGET_ARCH}" == 'x86_64' ]]; then
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-[0-9]*.jar "${DORIS_OUTPUT}/fe/lib/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-linux-ubuntu22-x86_64-[0-9]*.jar "${DORIS_OUTPUT}/fe/lib/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-sdk-[0-9]*.jar "${DORIS_OUTPUT}/fe/lib/jindofs"/
    elif [[ "${TARGET_SYSTEM}" == 'Linux' ]] && [[ "${TARGET_ARCH}" == 'aarch64' ]]; then
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-linux-el7-aarch64-[0-9]*.jar "${DORIS_OUTPUT}/fe/lib/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-sdk-[0-9]*.jar "${DORIS_OUTPUT}/fe/lib/jindofs"/
    fi

    cp -r -p "${DORIS_HOME}/minidump" "${DORIS_OUTPUT}/fe"/
    cp -r -p "${DORIS_HOME}/webroot/static" "${DORIS_OUTPUT}/fe/webroot"/

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/fe/webroot/static"/
    copy_common_files "${DORIS_OUTPUT}/fe/"
    mkdir -p "${DORIS_OUTPUT}/fe/log"
    mkdir -p "${DORIS_OUTPUT}/fe/doris-meta"
    mkdir -p "${DORIS_OUTPUT}/fe/conf/ssl"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/jdbc_drivers/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_udf/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/connectors/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/fe/plugins/java_extensions/"

    if [ "${TARGET_SYSTEM}" = "Darwin" ] || [ "${TARGET_SYSTEM}" = "Linux" ]; then
      mkdir -p "${DORIS_OUTPUT}/fe/arthas"
      rm -rf "${DORIS_OUTPUT}/fe/arthas/*"
      unzip -o "${DORIS_OUTPUT}/fe/lib/arthas-packaging-*.jar" arthas-bin.zip -d "${DORIS_OUTPUT}/fe/arthas/"
      unzip -o "${DORIS_OUTPUT}/fe/arthas/arthas-bin.zip" -d "${DORIS_OUTPUT}/fe/arthas/"
      rm "${DORIS_OUTPUT}/fe/arthas/math-game.jar"
      rm "${DORIS_OUTPUT}/fe/arthas/arthas-bin.zip"
    fi
fi

if [[ "${OUTPUT_BE_BINARY}" -eq 1 ]]; then
    # need remove old version hadoop jars if $DORIS_OUTPUT been used multiple times, otherwise will cause jar conflict
    rm -rf "${DORIS_OUTPUT}/be/lib/hadoop_hdfs"
    install -d "${DORIS_OUTPUT}/be/bin" \
        "${DORIS_OUTPUT}/be/conf" \
        "${DORIS_OUTPUT}/be/lib" \
        "${DORIS_OUTPUT}/be/www" \
        "${DORIS_OUTPUT}/be/tools/FlameGraph"

    cp -r -p "${DORIS_HOME}/bin"/*_be.sh "${DORIS_OUTPUT}/be/bin"/
    cp -r -p "${DORIS_HOME}/be/output/conf"/* "${DORIS_OUTPUT}/be/conf"/
    cp -r -p "${DORIS_HOME}/be/output/dict" "${DORIS_OUTPUT}/be/"

    if [[ -d "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" "${DORIS_OUTPUT}/be/lib/"
    fi

    if [[ -f "${DORIS_THIRDPARTY}/installed/lib/libz.so" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/lib/libz.so"* "${DORIS_OUTPUT}/be/lib/"
    fi

    if [[ "${BUILD_BE_JAVA_EXTENSIONS_FALSE_IN_CONF}" -eq 1 ]]; then
        echo -e "\033[33;1mWARNNING: \033[37;1mDisable Java UDF support in be.conf due to the BE was built without Java UDF.\033[0m"
        cat >>"${DORIS_OUTPUT}/be/conf/be.conf" <<EOF

# Java UDF and BE-JAVA-EXTENSION support
enable_java_support = false
EOF
    fi

    # Fix Killed: 9 error on MacOS (arm64).
    # See: https://stackoverflow.com/questions/67378106/mac-m1-cping-binary-over-another-results-in-crash
    if [[ -f "${DORIS_HOME}/be/output/lib/doris_be" ]]; then
        rm -f "${DORIS_OUTPUT}/be/lib/doris_be"
        cp -r -p "${DORIS_HOME}/be/output/lib/doris_be" "${DORIS_OUTPUT}/be/lib"/
    fi
    if [[ -d "${DORIS_HOME}/be/output/lib/doris_be.dSYM" ]]; then
        rm -rf "${DORIS_OUTPUT}/be/lib/doris_be.dSYM"
        cp -r "${DORIS_HOME}/be/output/lib/doris_be.dSYM" "${DORIS_OUTPUT}/be/lib"/
    fi
    if [[ -f "${DORIS_HOME}/be/output/lib/fs_benchmark_tool" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/fs_benchmark_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_META_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/meta_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_FILE_CACHE_MICROBENCH_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/file_cache_microbench" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_INDEX_TOOL}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/index_tool" "${DORIS_OUTPUT}/be/lib"/
    fi

    cp -r -p "${DORIS_HOME}/webroot/be"/* "${DORIS_OUTPUT}/be/www"/
    cp -r -p "${DORIS_HOME}/tools/FlameGraph"/* "${DORIS_OUTPUT}/be/tools/FlameGraph"/
    if [[ "${STRIP_DEBUG_INFO}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/debug_info" "${DORIS_OUTPUT}/be/lib"/
    fi

    if [[ "${BUILD_BENCHMARK}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/be/output/lib/benchmark_test" "${DORIS_OUTPUT}/be/lib/"/
    fi

    if [[ "${BUILD_FS_BENCHMARK}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/bin/run-fs-benchmark.sh" "${DORIS_OUTPUT}/be/bin/"/
    fi

    if [[ "${BUILD_TASK_EXECUTOR_SIMULATOR}" = "ON" ]]; then
        cp -r -p "${DORIS_HOME}/bin/run-task-executor-simulator.sh" "${DORIS_OUTPUT}/be/bin/"/
        cp -r -p "${DORIS_HOME}/be/output/lib/task_executor_simulator" "${DORIS_OUTPUT}/be/lib/"/
    fi

    extensions_modules=("java-udf")
    extensions_modules+=("jdbc-scanner")
    extensions_modules+=("hadoop-hudi-scanner")
    extensions_modules+=("paimon-scanner")
    extensions_modules+=("trino-connector-scanner")
    extensions_modules+=("max-compute-scanner")
    extensions_modules+=("avro-scanner")
    extensions_modules+=("lakesoul-scanner")
    extensions_modules+=("preload-extensions")
    extensions_modules+=("iceberg-metadata-scanner")

    if [[ -n "${BE_EXTENSION_IGNORE}" ]]; then
        IFS=',' read -r -a ignore_modules <<<"${BE_EXTENSION_IGNORE}"
        new_modules=()
        for module in "${extensions_modules[@]}"; do
            module=${module// /}
            if [[ -n "${module}" ]]; then
                ignore=0
                for ignore_module in "${ignore_modules[@]}"; do
                    if [[ "${module}" == "${ignore_module}" ]]; then
                        ignore=1
                        break
                    fi
                done
                if [[ "${ignore}" -eq 0 ]]; then
                    new_modules+=("${module}")
                fi
            fi
        done
        extensions_modules=("${new_modules[@]}")
    fi

    BE_JAVA_EXTENSIONS_DIR="${DORIS_OUTPUT}/be/lib/java_extensions/"
    rm -rf "${BE_JAVA_EXTENSIONS_DIR}"
    mkdir "${BE_JAVA_EXTENSIONS_DIR}"
    for extensions_module in "${extensions_modules[@]}"; do
        module_jar="${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/${extensions_module}-jar-with-dependencies.jar"
        module_proj_jar="${DORIS_HOME}/fe/be-java-extensions/${extensions_module}/target/${extensions_module}-project.jar"
        mkdir "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
        if [[ -f "${module_jar}" ]]; then
            cp "${module_jar}" "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
        fi
        if [[ -f "${module_proj_jar}" ]]; then
            cp "${module_proj_jar}" "${BE_JAVA_EXTENSIONS_DIR}"/"${extensions_module}"
        fi
    done

    # copy jindofs jars, only support for Linux x64 or arm
    install -d "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
    if [[ "${TARGET_SYSTEM}" == 'Linux' ]] && [[ "$TARGET_ARCH" == 'x86_64' ]]; then
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-[0-9]*.jar "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-linux-ubuntu22-x86_64-[0-9]*.jar "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-sdk-[0-9]*.jar "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
    elif [[ "${TARGET_SYSTEM}" == 'Linux' ]] && [[ "$TARGET_ARCH" == 'aarch64' ]]; then
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-core-linux-el7-aarch64-[0-9]*.jar "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
        cp -r -p "${DORIS_THIRDPARTY}"/installed/jindofs_libs/jindo-sdk-[0-9]*.jar "${DORIS_OUTPUT}/be/lib/java_extensions/jindofs"/
    fi

    cp -r -p "${DORIS_THIRDPARTY}/installed/webroot"/* "${DORIS_OUTPUT}/be/www"/
    copy_common_files "${DORIS_OUTPUT}/be/"
    mkdir -p "${DORIS_OUTPUT}/be/log"
    mkdir -p "${DORIS_OUTPUT}/be/log/pipe_tracing"
    mkdir -p "${DORIS_OUTPUT}/be/storage"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/jdbc_drivers/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_udf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/connectors/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/hadoop_conf/"
    mkdir -p "${DORIS_OUTPUT}/be/plugins/java_extensions/"
fi

if [[ "${BUILD_BROKER}" -eq 1 ]]; then
    install -d "${DORIS_OUTPUT}/apache_hdfs_broker"

    cd "${DORIS_HOME}/fs_brokers/apache_hdfs_broker"
    ./build.sh
    rm -rf "${DORIS_OUTPUT}/apache_hdfs_broker"/*
    cp -r -p "${DORIS_HOME}/fs_brokers/apache_hdfs_broker/output/apache_hdfs_broker"/* "${DORIS_OUTPUT}/apache_hdfs_broker"/
    copy_common_files "${DORIS_OUTPUT}/apache_hdfs_broker/"
    cd "${DORIS_HOME}"
fi

if [[ ${BUILD_CLOUD} -eq 1 ]]; then
    rm -rf "${DORIS_HOME}/output/ms"
    rm -rf "${DORIS_HOME}/cloud/output/lib/hadoop_hdfs"
    if [[ -d "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" ]]; then
        cp -r -p "${DORIS_THIRDPARTY}/installed/lib/hadoop_hdfs/" "${DORIS_HOME}/cloud/output/lib"
    fi
    cp -r -p "${DORIS_HOME}/cloud/output" "${DORIS_HOME}/output/ms"
fi

mkdir -p "${DORIS_HOME}/output/tools"
cp -r -p tools/fdb "${DORIS_HOME}/output/tools"

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ -n "${DORIS_POST_BUILD_HOOK}" ]]; then
    eval "${DORIS_POST_BUILD_HOOK}"
fi

exit 0
