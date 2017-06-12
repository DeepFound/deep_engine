#!/bin/sh

BUILD_DIR=$PWD/build
TARGET_DIR=$PWD/build/target

mkdir -p $TARGET_DIR
cd $BUILD_DIR

#
# 1st: configure
#
../configure --prefix=$TARGET_DIR --enable-minimal --with-pic
#../configure --prefix=$TARGET_DIR --enable-minimal --with-pic CXXFLAGS=-DTCMALLOC_DEEP

#
# 2nd: build
#
make
make install

