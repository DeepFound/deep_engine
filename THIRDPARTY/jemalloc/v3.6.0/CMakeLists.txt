cmake_minimum_required(VERSION 2.8)

project(deepis-jemalloc)

set(TOP ${CMAKE_SOURCE_DIR}/../../..)

include(${TOP}/build/cmake/Common.cmake)

if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
	set(DEEPIS_JEMALLOC_LIB ${CMAKE_SOURCE_DIR}/Release/libjemalloc.lib)
	set(DEEPIS_JEMALLOC_DYLIB ${CMAKE_SOURCE_DIR}/Release/libjemalloc.${DYLIB_EXTENSION})
	set(DEEPIS_JEMALLOC_INC ${CMAKE_SOURCE_DIR}/src/jemalloc)

elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
	# TODO: Dynamic linking on mac, will need to productize
	set(DEEPIS_JEMALLOC_LIB ${CMAKE_SOURCE_DIR}/build/target/lib/libjemalloc_pic.a)
	set(DEEPIS_JEMALLOC_DYLIB ${CMAKE_SOURCE_DIR}/build/target/lib/libjemalloc.${DYLIB_EXTENSION})
	set(DEEPIS_JEMALLOC_INC ${CMAKE_SOURCE_DIR}/build/target/include/jemalloc)

else()
	set(DEEPIS_JEMALLOC_LIB ${CMAKE_SOURCE_DIR}/build/target/lib/libjemalloc_pic.a)
	set(DEEPIS_JEMALLOC_DYLIB ${CMAKE_SOURCE_DIR}/build/target/lib/libjemalloc.${DYLIB_EXTENSION})
	set(DEEPIS_JEMALLOC_INC ${CMAKE_SOURCE_DIR}/build/target/include/jemalloc)
endif()

#
# Export package
#
set(JEMALLOC_CONFIG_FILE ${REPOSITORY}/Find${PROJECT_NAME}.cmake)

file(WRITE ${JEMALLOC_CONFIG_FILE} "set(DEEPIS_JEMALLOC_LIB ${DEEPIS_JEMALLOC_LIB})\n")
file(APPEND ${JEMALLOC_CONFIG_FILE} "set(DEEPIS_JEMALLOC_DYLIB ${DEEPIS_JEMALLOC_DYLIB})\n")
file(APPEND ${JEMALLOC_CONFIG_FILE} "set(DEEPIS_JEMALLOC_INC ${DEEPIS_JEMALLOC_INC})\n")
