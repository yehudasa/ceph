#!/bin/bash
# -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
# vim: ts=8 sw=2 smarttab
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2012 Inktank, Inc.
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation. See file COPYING.
#

PID_DIR=out
DEV_DIR=dev
OUT_DIR=out
ASOK_DIR=out

if [[ $1 != "--do-it" ]]; then
  echo " This test assumes the following:"
  echo "   - there are three monitor configured (a, b and c)"
  echo "   - running the monitors will lead to a healthy cluster"
  echo "   - there are no monitors running"
  echo
  echo "   - data dirs are in     `pwd`/$DEV_DIR"
  echo "   - log dir is           `pwd`/$OUT_DIR"
  echo "   - pid files are in     `pwd`/$PID_DIR"
  echo "   - admin sockets are in `pwd`/$ASOK_DIR"
  echo
  echo "   - the 'ceph' tool is in `pwd`/ceph"
  echo "   - 'ceph-mon' is in `pwd`/ceph-mon"
  echo
  echo "   - we will remove, create and potentially corrupt your data"
  echo
  echo " If you understand and still want to run the test, please run it with"
  echo "   --do-it"
  echo " as the first argument"
  echo
  echo " Yours Truly,"
  echo "   The Cephalopod attempting to break your system in name of science"
  echo
  exit 0
fi

check_assumptions() {

  error=0

  for d in $DEV_DIR $OUT_DIR $PID_DIR $ASOK_DIR; do
    if [[ ! -e "$d" ]]; then
      echo "  - `pwd`/$d does not exist"
      error=1
    elif [[ ! -d "$d" ]]; then
      echo "  - `pwd`/$d is not a directory"
      error=1
    fi
  done

  for binary in ceph ceph-mon; do
    if [[ ! -e "$binary" ]]; then
      echo "  - `pwd`/$binary does not exist"
      error=1
    elif [[ ! -x "$binary" ]]; then
      echo "  - `pwd`/$binary is not executable"
      error=1
    fi
  done

  if [[ $error -eq 1 ]]; then
    exit 1
  fi

  for m in a b c; do
    if [[ ! -e "$DEV_DIR/mon.$m" ]]; then
      echo "  - $DEV_DIR/mon.$m does not exist"
      exit 1
    elif [[ ! -d "$DEV_DIR/mon.$m" ]]; then
      echo "  - $DEV_DIR/mon.$m is not a directory"
      exit 1
    elif [[ ! -d "$DEV_DIR/mon.$m/store.db" ]]; then
      echo "  - $DEV_DIR/mon.$m does not have an initialized store"
      exit 1
    fi
  done
}

declare -A mon_pid

check_assumptions ;
#
# force a monitor to synchronize with the cluster
#
sync() {
  mon_id=$1

  echo "Force sync of mon.$mon_id"

  fsid=`./ceph fsid`
  # kill mon if it is running
  if [ -e "$OUT_DIR/mon.$mon_id.pid" ]; then
    mon_pid=`cat out/mon.$mon_id.pid`
    echo "Killing mon.$mon_id (pid $mon_pid)"
    kill -9 $mon_pid
    rm $OUT_DIR/mon.$mon_id.pid
  fi

  rm $OUT_DIR/mon.$mon_id.log
  rm -fr $OUT_DIR/mon.$mon_id/*

  ./ceph-mon -i $mon_id -c ceph.conf --mkfs --fsid $fsid
  if [[ $? -ne 0 ]]; then
    echo "  - Error creating bare state for mon.$mon_id"
    exit 1
  fi

  ./ceph-mon -i $mon_id -c ceph.conf \
    --pid-file $PID_DIR/mon.$mon_id.pid --mon-sync-debug

  if [[ $? -ne 0 ]]; then
    echo "  - Error starting mon.$mon_id"
    exit 1
  fi


}






