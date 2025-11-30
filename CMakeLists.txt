cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME http_request)

project(${TARGET_NAME})

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package.
# For clang-tidy checks, we make these optional as they're not needed for static analysis
if(CLANG_TIDY)
  find_package(OpenSSL)
  find_package(CURL)
else()
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/httplib)

set(EXTENSION_SOURCES src/http_request_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Strip symbols in release builds to reduce extension size
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  if(APPLE)
    add_custom_command(TARGET ${LOADABLE_EXTENSION_NAME} POST_BUILD
      COMMAND strip -x $<TARGET_FILE:${LOADABLE_EXTENSION_NAME}>
      COMMENT "Stripping symbols from ${LOADABLE_EXTENSION_NAME}")
  elseif(UNIX)
    add_custom_command(TARGET ${LOADABLE_EXTENSION_NAME} POST_BUILD
      COMMAND strip --strip-unneeded $<TARGET_FILE:${LOADABLE_EXTENSION_NAME}>
      COMMENT "Stripping symbols from ${LOADABLE_EXTENSION_NAME}")
  endif()
endif()

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
