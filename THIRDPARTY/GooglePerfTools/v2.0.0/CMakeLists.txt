cmake_minimum_required(VERSION 2.8)

project(deepis-google-perftools)

set(TOP ${CMAKE_SOURCE_DIR}/../../..)

include(${TOP}/build/cmake/Common.cmake)

if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
	set(DEEPIS_GOOGLE_PERFTOOLS_LIB ${CMAKE_SOURCE_DIR}/Release/libtcmalloc_minimal.lib)
	set(DEEPIS_GOOGLE_PERFTOOLS_DYLIB ${CMAKE_SOURCE_DIR}/Release/libtcmalloc_minimal.${DYLIB_EXTENSION})
	set(DEEPIS_GOOGLE_PERFTOOLS_INC ${CMAKE_SOURCE_DIR}/src/gperftools)

elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
	# TODO: Dynamic linking on mac, will need to productize
	set(DEEPIS_GOOGLE_PERFTOOLS_LIB ${CMAKE_SOURCE_DIR}/build/target/lib/libtcmalloc_minimal.${DYLIB_EXTENSION})
	set(DEEPIS_GOOGLE_PERFTOOLS_DYLIB ${CMAKE_SOURCE_DIR}/build/target/lib/libtcmalloc_minimal.${DYLIB_EXTENSION})
	set(DEEPIS_GOOGLE_PERFTOOLS_INC ${CMAKE_SOURCE_DIR}/build/target/include/gperftools)

else()
	set(DEEPIS_GOOGLE_PERFTOOLS_LIB ${CMAKE_SOURCE_DIR}/build/target/lib/libtcmalloc_minimal.a)
	set(DEEPIS_GOOGLE_PERFTOOLS_DYLIB ${CMAKE_SOURCE_DIR}/build/target/lib/libtcmalloc_minimal.${DYLIB_EXTENSION})
	set(DEEPIS_GOOGLE_PERFTOOLS_INC ${CMAKE_SOURCE_DIR}/build/target/include/gperftools)
endif()

#
# Export package
#
set(GOOGLE_PERFTOOLS_CONFIG_FILE ${REPOSITORY}/Find${PROJECT_NAME}.cmake)

file(WRITE ${GOOGLE_PERFTOOLS_CONFIG_FILE} "set(DEEPIS_GOOGLE_PERFTOOLS_LIB ${DEEPIS_GOOGLE_PERFTOOLS_LIB})\n")
file(APPEND ${GOOGLE_PERFTOOLS_CONFIG_FILE} "set(DEEPIS_GOOGLE_PERFTOOLS_DYLIB ${DEEPIS_GOOGLE_PERFTOOLS_DYLIB})\n")
file(APPEND ${GOOGLE_PERFTOOLS_CONFIG_FILE} "set(DEEPIS_GOOGLE_PERFTOOLS_INC ${DEEPIS_GOOGLE_PERFTOOLS_INC})\n")
