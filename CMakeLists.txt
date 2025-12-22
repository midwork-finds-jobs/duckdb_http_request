cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME http_request)

project(${TARGET_NAME})

# OpenSSL is needed for HTTPS support via httplib
# For clang-tidy checks, we make it optional as it's not needed for static analysis
if(CLANG_TIDY)
  find_package(OpenSSL)
else()
  find_package(OpenSSL REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/httplib)

set(EXTENSION_SOURCES src/http_request_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
if(NOT CLANG_TIDY)
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
