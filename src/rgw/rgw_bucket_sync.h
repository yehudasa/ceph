
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_common.h"

class RGWSI_Zone;


class RGWBucketSyncPolicyHandler {
  RGWSI_Zone *zone_svc;
  RGWBucketInfo bucket_info;

  std::set<string> source_zones;

  struct peer_info {
    std::string type;
    rgw_bucket bucket;
    /* need to have config for other type of sources */

    bool operator<(const peer_info& si) const {
      if (type == si.type) {
        return (bucket < si.bucket);
      }
      return (type < si.type);
    }
  };

  std::map<string, std::set<peer_info> > sources;
  std::map<string, std::set<peer_info> > targets;

public:
  RGWBucketSyncPolicyHandler(RGWSI_Zone *_zone_svc,
                             RGWBucketInfo& _bucket_info);

  int init();

  const RGWBucketInfo& get_bucket_info() const {
    return bucket_info;
  }

  bool zone_is_source(const string& zone_id) const {
    return sources.find(zone_id) != sources.end();
  }

  bool bucket_is_sync_source() const {
    return !targets.empty();
  }

  bool bucket_is_sync_target() const {
    return !sources.empty();
  }

  bool bucket_exports_data() const;
  bool bucket_imports_data() const;
};

