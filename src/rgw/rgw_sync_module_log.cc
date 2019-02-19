// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_log.h"

#define dout_subsys ceph_subsys_rgw

class RGWLogStatRemoteObjCBCR : public RGWStatRemoteObjCBCR {
public:
  RGWLogStatRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          rgw_bucket& _src_bucket, rgw_obj_key& _key) : RGWStatRemoteObjCBCR(_sync_env, _src_bucket, _key) {}
  int operate() override {
    ldout(sync_env->cct, 0) << "SYNC_LOG: stat of remote obj: z=" << sync_env->source_zone
                            << " b=" << src_bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                            << " attrs=" << attrs << dendl;
    return set_cr_done();
  }

};

class RGWLogStatRemoteObjCR : public RGWCallStatRemoteObjCR {
public:
  RGWLogStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        rgw_bucket& _src_bucket, rgw_obj_key& _key) : RGWCallStatRemoteObjCR(_sync_env, _src_bucket, _key) {
  }

  ~RGWLogStatRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWLogStatRemoteObjCBCR(sync_env, src_bucket, key);
  }
};

class RGWLogDataSyncModule : public RGWDataSyncModule {
  string prefix;
public:
  explicit RGWLogDataSyncModule(const string& _prefix) : prefix(_prefix) {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: sync_object: b=" << sync_pipe.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch.value_or(0) << dendl;
    return new RGWLogStatRemoteObjCR(sync_env, sync_pipe.source_bs.bucket, key);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: rm_object: b=" << sync_pipe.source_bs.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: create_delete_marker: b=" << sync_pipe.source_bs.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWLogSyncModuleInstance : public RGWSyncModuleInstance {
  RGWLogDataSyncModule data_handler;
public:
  explicit RGWLogSyncModuleInstance(const string& prefix) : data_handler(prefix) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWLogSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
  string prefix = config["prefix"];
  instance->reset(new RGWLogSyncModuleInstance(prefix));
  return 0;
}

