set(curl_VERSION 8.8.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(curl_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/curl-8_8_0.zip")
  set(curl_SHA256 "73c70c94f487c5ae26f9f27094249e40bb1667ae6c0406a75c3b11f86f0c1128")
else()
  # Pin the public archive to the commit referenced by the curl-8_8_0 tag.
  set(curl_URL "https://codeload.github.com/curl/curl/zip/fd567d4f06857f4fc8e2f64ea727b1318f76ad33")
  set(curl_SHA256 "5531e2840a045b401231a7b9af0ff53b96fe77e5ff5e2b1f2e89a7a51275e690")
endif()

set(curl_CMAKE_OPTIONS
    -DCMAKE_CXX_STANDARD=11
    -DOPENSSL_ROOT_DIR:PATH=${OpenSSL_ROOT})

set(curl_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

if (curl_VERSION STREQUAL "8.8.0")
  set(curl_PATCHES
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-6197-fix-CVE-2024-6197-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-6874-fix-CVE-2024-6874-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-7264-fix-CVE-2024-7264-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-8096-fix-CVE-2024-8096-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-9681-fix-CVE-2024-9681-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-11053-fix-CVE-2024-11053-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2025-0167-fix-CVE-2025-0167-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2025-0725-fix-CVE-2025-0725-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/support_old_cmake.patch
  )
endif()

add_thirdparty_lib(CURL
        URL ${curl_URL}
        SHA256 ${curl_SHA256}
        FAKE_SHA256 ${curl_FAKE_SHA256}
        VERSION ${curl_VERSION}
        CONF_OPTIONS ${curl_CMAKE_OPTIONS}
        C_FLAGS ${curl_C_FLAGS}
        PATCHES ${curl_PATCHES})

find_package(CURL ${curl_VERSION} REQUIRED PATHS ${CURL_ROOT} CONFIG)
