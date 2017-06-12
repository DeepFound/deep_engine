cmake_minimum_required(VERSION 2.6)

project(deepis-db-common)

set(TOP ${CMAKE_SOURCE_DIR}/../..)

include(${TOP}/build/cmake/Common.cmake)

#
# Import packages
#


#
# Includes
#
set(DEEPIS_DB_COMMON_INC ${CMAKE_SOURCE_DIR}/src/main/native)

#
# Source Files
#


#
# Create library
#


#
# Export package
#
set(DB_COMMON_CONFIG_FILE ${REPOSITORY}/Find${PROJECT_NAME}.cmake)

file(WRITE ${DB_COMMON_CONFIG_FILE} "set(DEEPIS_DB_COMMON_INC ${DEEPIS_DB_COMMON_INC})\n")

#
# Test
#
