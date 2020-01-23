
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "svc_meta_be.h"
#include "svc_bucket_sync.h"

class RGWSI_Zone;
class RGWSI_SysObj_Cache;
class RGWSI_Bucket_SObj;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_Bucket_Sync_SObj : public RGWSI_Bucket_Sync
{
  struct bucket_sync_policy_cache_entry {
    std::shared_ptr<RGWBucketSyncPolicyHandler> handler;
  };

  using RGWChainedCacheImpl_bucket_sync_policy_cache_entry = RGWChainedCacheImpl<bucket_sync_policy_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_bucket_sync_policy_cache_entry> sync_policy_cache;

  class HintIndexManager {
    struct {
      RGWSI_Zone *zone;
      RGWSI_SysObj *sysobj;
    } svc;

  public:
    HintIndexManager() {}

    void init(RGWSI_Zone *_zone_svc,
              RGWSI_SysObj *_sysobj_svc) {
      svc.zone = _zone_svc;
      svc.sysobj = _sysobj_svc;
    }

    rgw_raw_obj get_sources_obj(const rgw_bucket& bucket) const;
    rgw_raw_obj get_dests_obj(const rgw_bucket& bucket) const;
  } hint_index_mgr;

  int do_start() override;

  int do_update_hints(const RGWBucketInfo& bucket_info,
                      std::vector<rgw_bucket>& added_dests,
                      std::vector<rgw_bucket>& removed_dests,
                      std::vector<rgw_bucket>& added_sources,
                      std::vector<rgw_bucket>& removed_sources,
                      optional_yield y);
public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Bucket_SObj *bucket_sobj{nullptr};
  } svc;

  RGWSI_Bucket_Sync_SObj(CephContext *cct) : RGWSI_Bucket_Sync(cct) {}
  ~RGWSI_Bucket_Sync_SObj();

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj *_sysobj_svc,
            RGWSI_SysObj_Cache *_cache_svc,
            RGWSI_Bucket_SObj *_bucket_sobj_svc);


  int get_policy_handler(RGWSI_Bucket_BI_Ctx& ctx,
                         std::optional<string> zone,
                         std::optional<rgw_bucket> bucket,
                         RGWBucketSyncPolicyHandlerRef *handler,
                         optional_yield y) override;

  int handle_bi_update(RGWBucketInfo& bucket_info,
                       RGWBucketInfo *orig_bucket_info,
                       optional_yield y) override;
  int handle_bi_removal(const RGWBucketInfo& bucket_info,
                        optional_yield y) override;

  int get_bucket_sync_hints(const rgw_bucket& bucket,
                            std::set<rgw_bucket> *sources,
                            std::set<rgw_bucket> *dests,
                            optional_yield y) override;
};

