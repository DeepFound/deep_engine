#!/bin/sh

BUILD_DIR=$PWD/build
TARGET_DIR=$PWD/build/target

mkdir -p $TARGET_DIR
cd $BUILD_DIR

#
# 1st: configure
#
../configure --prefix=$TARGET_DIR --with-private-namespace=ct_

#
# 2nd: build
#
make
make install_bin install_include install_lib

