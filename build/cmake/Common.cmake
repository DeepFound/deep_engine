#
# Definitions
#
set(REPOSITORY "$ENV{HOME}/.cmake/deepis")
set(CMAKE_MODULE_PATH "${REPOSITORY}")

#
# Build type and flags
#
include(${TOP}/build/cmake/BuildType.cmake)
include(${TOP}/build/cmake/Defines.cmake)

#
# Compiler
#
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)

#
# Verbose make output
#
set(CMAKE_VERBOSE_MAKEFILE ON)

#
# Allow asserts
#
string(REGEX REPLACE "-DNDEBUG" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
string(REGEX REPLACE "/D NDEBUG" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})

#
# Allow a static lib to be compiled into a shared lib on 64b system
#
if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
endif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")

message(STATUS "Build is Linux")
set(DYLIB_EXTENSION "so")
link_libraries("rt")
link_libraries("pthread")

#
# DEEP_DEBUG flag
#
if(DEEP_DEBUG)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_DEBUG")
else()
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEEP_DEBUG")
endif()

#
# Compression
#
link_libraries("z")

#
# tinyxml
#
if (NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-tinyxml" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-google-perftools" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-jemalloc")
	find_package(deepis-tinyxml REQUIRED)

	set(DEEPIS_STANDARD_INCS ${DEEPIS_TINYXML_INC})
	set(DEEPIS_STANDARD_LIBS ${DEEPIS_TINYXML_LIB})
endif()

#
# tcmalloc
#
if (CMAKE_CXX_FLAGS MATCHES "TCMALLOC" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-google-perftools" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-jemalloc")
	message(STATUS "Building with tcmalloc")

	find_package(deepis-google-perftools REQUIRED)

	set(DEEPIS_STANDARD_LIBS ${DEEPIS_STANDARD_LIBS} ${DEEPIS_GOOGLE_PERFTOOLS_LIB})

	set(DEEPIS_STANDARD_INCS ${DEEPIS_STANDARD_INCS} ${DEEPIS_GOOGLE_PERFTOOLS_INC})

	#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
endif()

#
# jemalloc
#
if (CMAKE_CXX_FLAGS MATCHES "JEMALLOC" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-jemalloc" AND NOT ${CMAKE_PROJECT_NAME} STREQUAL "deepis-google-perftools")
	message(STATUS "Building with jemalloc")

	find_package(deepis-jemalloc REQUIRED)

	set(DEEPIS_STANDARD_LIBS ${DEEPIS_STANDARD_LIBS} ${DEEPIS_JEMALLOC_LIB})

	set(DEEPIS_STANDARD_INCS ${DEEPIS_STANDARD_INCS} ${DEEPIS_JEMALLOC_INC})
endif()


#
# Testing
#
include(CTest)
enable_testing()

macro(add_deep_test testName srcFile)
	if(DEEP_TEST)
		add_executable(${testName} ${srcFile})
		target_link_libraries(${testName} ${ARGN})
		add_test(${testName} ${testName})
	endif()
endmacro()
