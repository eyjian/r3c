# Writed by yijian (eyjian@qq.com)

# 2.8.11
cmake_minimum_required(VERSION 2.8.11)
project(r3c)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_BUILD_TYPE "Debug")

add_definitions(-DSLEEP_USE_POLL)
#add_definitions(-DR3C_TEST)
add_definitions(-D__STDC_FORMAT_MACROS=1)
add_definitions(-D__STDC_CONSTANT_MACROS=1)
#add_definitions(-O2)
add_definitions(-g -ggdb -fPIC -Wall -W -Wwrite-strings -Wno-missing-field-initializers)

if (HIREDIS_HOME)
    set(HAVE_HIREDIS 1 CACHE INTERNAL HAVE_HIREDIS)
    include_directories(${HIREDIS_HOME}/include)
    link_directories(${HIREDIS_HOME}/lib)
else ()
    # hiredis.h
    find_path(
        HIREDIS_INCLUDE
        NAMES
        hiredis.h
        PATHS 
        /usr/local/hiredis/include/hiredis
        /usr/local/thirdparty/hiredis/include/hiredis
        /usr/include
        NO_DEFAULT_PATH
    )
    # libhiredis.a
    find_path(
        HIREDIS_LIB
        NAMES
        libhiredis.a
        PATHS 
        /usr/local/hiredis/lib
        /usr/local/thirdparty/hiredis/lib
        /usr/lib
        NO_DEFAULT_PATH
    )
        
    if ("${HIREDIS_INCLUDE}" STREQUAL "HIREDIS_INCLUDE-NOTFOUND" OR "${HIREDIS_LIB}" STREQUAL "HIREDIS_INCLUDE-NOTFOUND")
        set(HAVE_HIREDIS 0 CACHE INTERNAL HAVE_HIREDIS)
        include_directories(/usr/local/hiredis/include)
        link_directories(/usr/local/hiredis/lib)
    else ()
        set(HAVE_HIREDIS 1 CACHE INTERNAL HAVE_HIREDIS)
        include_directories(${HIREDIS_INCLUDE}/..)
        link_directories(${HIREDIS_LIB})
    endif ()
endif ()

# deps
if (NOT HAVE_HIREDIS)
    add_subdirectory(deps)
endif ()

# libr3c.a
add_library(
    r3c
    STATIC
    r3c.cpp 
    utils.cpp
    sha1.cpp
)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
link_directories(${CMAKE_CURRENT_SOURCE_DIR})

# tests
add_subdirectory(tests)

# 安装（-DCMAKE_INSTALL_PREFIX）
install(
        TARGETS
        r3c
        DESTINATION lib
)
install(
        FILES r3c.h
        DESTINATION include/r3c
)
