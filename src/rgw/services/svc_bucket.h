
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
#include "svc_bucket_types.h"

class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;

struct rgw_cache_entry_info;

template <class T>
class RGWChainedCacheImpl;

class RGWSI_Bucket : public RGWServiceInstance
{
  friend class Instance;

  struct bucket_info_cache_entry {
    RGWBucketInfo info;
    real_time mtime;
    map<string, bufferlist> attrs;
  };

  using RGWChainedCacheImpl_bucket_info_cache_entry = RGWChainedCacheImpl<bucket_info_cache_entry>;
  unique_ptr<RGWChainedCacheImpl_bucket_info_cache_entry> binfo_cache;

  RGWSI_Bucket_BE_Handler ep_be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> ep_be_module;
  RGWSI_BucketInstance_BE_Handler bi_be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> bi_be_module;

  int do_start() override;

  int do_read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                   const string& key,
                                   RGWBucketInfo *info,
                                   real_time *pmtime,
                                   map<string, bufferlist> *pattrs,
                                   rgw_cache_entry_info *cache_info,
                                   boost::optional<obj_version> refresh_version);

  int read_bucket_stats(const RGWBucketInfo& bucket_info,
                        RGWBucketEnt *ent);

  int handle_bucket_overwrite(RGWSI_Bucket_BI_Ctx& ctx,
                              const string& key,
                              const RGWBucketInfo& info,
                              const RGWBucketInfo& orig_info);
public:
  struct Svc {
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
    RGWSI_SysObj_Cache *cache{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SyncModules *sync_modules{nullptr};
  } svc;

  RGWSI_Bucket(CephContext *cct);
  ~RGWSI_Bucket();

  static string get_entrypoint_meta_key(const rgw_bucket& bucket);
  static string get_bi_meta_key(const rgw_bucket& bucket);

  RGWSI_Bucket_BE_Handler& get_ep_be_handler() {
    return ep_be_handler;
  }

  RGWSI_BucketInstance_BE_Handler& get_bi_be_handler() {
    return bi_be_handler;
  }

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_SysObj *_sysobj_svc,
	    RGWSI_SysObj_Cache *_cache_svc,
            RGWSI_BucketIndex *_bi,
            RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SyncModules *_sync_modules);


  int read_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                  const string& key,
                                  RGWBucketEntryPoint *entry_point,
                                  RGWObjVersionTracker *objv_tracker,
                                  real_time *pmtime,
                                  map<string, bufferlist> *pattrs,
                                  rgw_cache_entry_info *cache_info = nullptr,
                                  boost::optional<obj_version> refresh_version = boost::none);

  int store_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                   const string& key,
                                   RGWBucketEntryPoint& info,
                                   bool exclusive,
                                   real_time mtime,
                                   map<string, bufferlist> *pattrs,
                                   RGWObjVersionTracker *objv_tracker);

  int remove_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                    const string& key,
                                    RGWObjVersionTracker *objv_tracker);

  int read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                const string& key,
                                RGWBucketInfo *info,
                                real_time *pmtime,
                                map<string, bufferlist> *pattrs,
                                rgw_cache_entry_info *cache_info = nullptr,
                                boost::optional<obj_version> refresh_version = boost::none);

  int read_bucket_info(RGWSI_Bucket_X_Ctx& ep_ctx,
                       const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       real_time *pmtime,
                       map<string, bufferlist> *pattrs,
                       boost::optional<obj_version> refresh_version);

  int store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                 const string& key,
                                 RGWBucketInfo& info,
                                 std::optional<RGWBucketInfo *> orig_info, /* nullopt: orig_info was not fetched,
                                                                              nullptr: orig_info was not found (new bucket instance */
                                 bool exclusive,
                                 real_time mtime,
                                 map<string, bufferlist> *pattrs);

  int remove_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                  const string& key,
                                  RGWObjVersionTracker *objv_tracker);

  int read_bucket_stats(RGWSI_Bucket_X_Ctx& ctx,
                        const rgw_bucket& bucket,
                        RGWBucketEnt *ent);

  int read_buckets_stats(RGWSI_Bucket_X_Ctx& ctx,
                         map<string, RGWBucketEnt>& m);
};

