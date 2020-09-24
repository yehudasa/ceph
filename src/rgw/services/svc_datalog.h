// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw/rgw_service.h"


class RGWDataChangesLog;
class RGWDataChangesLogInfo;
struct RGWDataChangesLogMarker;
struct rgw_data_change_log_entry;

namespace rgw {
  class BucketChangeObserver;
}

class RGWSI_DataLog : public RGWServiceInstance
{
public:
  RGWSI_DataLog(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_DataLog() {}

  virtual RGWDataChangesLog *get_log() = 0;

  virtual void set_observer(rgw::BucketChangeObserver *observer) = 0;

  virtual int get_log_shard_id(rgw_bucket& bucket, int shard_id) = 0;
  virtual int calc_shard_id(rgw_bucket& bucket, int shard_id, int num_datalog_shards) = 0;

  virtual int get_info(int shard_id, RGWDataChangesLogInfo *info) = 0;

  virtual int add_entry(const RGWBucketInfo& bucket_info, int shard_id) = 0;
  virtual int list_entries(int shard, const real_time& start_time, const real_time& end_time, int max_entries,
                           list<rgw_data_change_log_entry>& entries,
                           const string& marker,
                           string *out_marker,
                           bool *truncated) = 0;
  virtual int list_entries(const real_time& start_time, const real_time& end_time, int max_entries,
                           list<rgw_data_change_log_entry>& entries, RGWDataChangesLogMarker& marker, bool *ptruncated) = 0;
  virtual int trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                           const string& start_marker, const string& end_marker) = 0;
};


