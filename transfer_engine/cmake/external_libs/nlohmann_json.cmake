if (TARGET nlohmann_json::nlohmann_json)
    return()
endif()

set(NLOHMANN_JSON_VERSION 3.11.3)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(NLOHMANN_JSON_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v3.11.3.zip")
else()
    set(NLOHMANN_JSON_URL "https://gitee.com/mirrors/nlohmann-json/repository/archive/v3.11.3.zip")
endif()
set(NLOHMANN_JSON_SHA256 "0deac294b2c96c593d0b7c0fb2385a2f4594e8053a36c52b11445ef4b9defebb")

TE_ADD_THIRDPARTY_LIB(NLOHMANN_JSON
    URL ${NLOHMANN_JSON_URL}
    SHA256 ${NLOHMANN_JSON_SHA256}
    VERSION ${NLOHMANN_JSON_VERSION}
    CONF_OPTIONS
        -DJSON_BuildTests=OFF
        -DCMAKE_BUILD_TYPE=Release
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS})

set(nlohmann_json_DIR ${NLOHMANN_JSON_ROOT}/share/cmake/nlohmann_json)
if (EXISTS "${NLOHMANN_JSON_ROOT}/lib/cmake/nlohmann_json")
    set(nlohmann_json_DIR ${NLOHMANN_JSON_ROOT}/lib/cmake/nlohmann_json)
elseif(EXISTS "${NLOHMANN_JSON_ROOT}/lib64/cmake/nlohmann_json")
    set(nlohmann_json_DIR ${NLOHMANN_JSON_ROOT}/lib64/cmake/nlohmann_json)
endif()
find_package(nlohmann_json ${NLOHMANN_JSON_VERSION} REQUIRED PATHS "${nlohmann_json_DIR}" NO_DEFAULT_PATH)
