// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"

struct RGWServices_Def;

class RGWServiceInstance
{
  friend struct RGWServices_Def;

protected:
  CephContext *cct;

  enum StartState {
    StateInit = 0,
    StateStarting = 1,
    StateStarted = 2,
  } start_state{StateInit};

  virtual void shutdown() {}
  virtual int do_start() {
    return 0;
  }
public:
  RGWServiceInstance(CephContext *_cct) : cct(_cct) {}
  virtual ~RGWServiceInstance() {}

  int start();
  bool is_started() {
    return (start_state == StateStarted);
  }

  CephContext *ctx() {
    return cct;
  }
};

class RGWSI_Finisher;
class RGWSI_Bucket;
class RGWSI_BucketIndex;
class RGWSI_BucketIndex_RADOS;
class RGWSI_Cls;
class RGWSI_MDLog;
class RGWSI_Meta;
class RGWSI_MetaBackend;
class RGWSI_MetaBackend_SObj;
class RGWSI_MetaBackend_OTP;
class RGWSI_Notify;
class RGWSI_OTP;
class RGWSI_RADOS;
class RGWSI_Zone;
class RGWSI_ZoneUtils;
class RGWSI_Quota;
class RGWSI_SyncModules;
class RGWSI_SysObj;
class RGWSI_SysObj_Core;
class RGWSI_SysObj_Cache;
class RGWSI_User;
class RGWSI_User_RADOS;

struct RGWServices_Def
{
  bool can_shutdown{false};
  bool has_shutdown{false};

  std::unique_ptr<RGWSI_Finisher> finisher;
  std::unique_ptr<RGWSI_Bucket> bucket;
  std::unique_ptr<RGWSI_BucketIndex_RADOS> bi_rados;
  std::unique_ptr<RGWSI_Cls> cls;
  std::unique_ptr<RGWSI_MDLog> mdlog;
  std::unique_ptr<RGWSI_Meta> meta;
  std::unique_ptr<RGWSI_MetaBackend_SObj> meta_be_sobj;
  std::unique_ptr<RGWSI_MetaBackend_OTP> meta_be_otp;
  std::unique_ptr<RGWSI_Notify> notify;
  std::unique_ptr<RGWSI_OTP> otp;
  std::unique_ptr<RGWSI_RADOS> rados;
  std::unique_ptr<RGWSI_Zone> zone;
  std::unique_ptr<RGWSI_ZoneUtils> zone_utils;
  std::unique_ptr<RGWSI_Quota> quota;
  std::unique_ptr<RGWSI_SyncModules> sync_modules;
  std::unique_ptr<RGWSI_SysObj> sysobj;
  std::unique_ptr<RGWSI_SysObj_Core> sysobj_core;
  std::unique_ptr<RGWSI_SysObj_Cache> sysobj_cache;
  std::unique_ptr<RGWSI_User_RADOS> user_rados;

  RGWServices_Def();
  ~RGWServices_Def();

  int init(CephContext *cct, bool have_cache, bool raw_storage);
  void shutdown();
};


struct RGWServices
{
  RGWServices_Def _svc;

  CephContext *cct;

  RGWSI_Finisher *finisher{nullptr};
  RGWSI_Bucket *bucket{nullptr};
  RGWSI_BucketIndex *bi{nullptr};
  RGWSI_Cls *cls{nullptr};
  RGWSI_MDLog *mdlog{nullptr};
  RGWSI_Meta *meta{nullptr};
  RGWSI_MetaBackend *meta_be_sobj{nullptr};
  RGWSI_MetaBackend *meta_be_otp{nullptr};
  RGWSI_Notify *notify{nullptr};
  RGWSI_OTP *otp{nullptr};
  RGWSI_RADOS *rados{nullptr};
  RGWSI_Zone *zone{nullptr};
  RGWSI_ZoneUtils *zone_utils{nullptr};
  RGWSI_Quota *quota{nullptr};
  RGWSI_SyncModules *sync_modules{nullptr};
  RGWSI_SysObj *sysobj{nullptr};
  RGWSI_SysObj_Cache *cache{nullptr};
  RGWSI_SysObj_Core *core{nullptr};
  RGWSI_User *user{nullptr};

  int do_init(CephContext *cct, bool have_cache, bool raw_storage);

  int init(CephContext *cct, bool have_cache) {
    return do_init(cct, have_cache, false);
  }

  int init_raw(CephContext *cct, bool have_cache) {
    return do_init(cct, have_cache, true);
  }
  void shutdown() {
    _svc.shutdown();
  }
};

class RGWMetadataManager;
class RGWMetadataHandler;
class RGWUserCtl;
class RGWBucketCtl;
class RGWOTPCtl;

struct RGWCtlDef {
  struct _meta {
    std::unique_ptr<RGWMetadataManager> mgr;
    std::unique_ptr<RGWMetadataHandler> bucket;
    std::unique_ptr<RGWMetadataHandler> bucket_instance;
    std::unique_ptr<RGWMetadataHandler> user;
    std::unique_ptr<RGWMetadataHandler> otp;

    _meta();
    ~_meta();
  } meta;

  std::unique_ptr<RGWUserCtl> user;
  std::unique_ptr<RGWBucketCtl> bucket;
  std::unique_ptr<RGWOTPCtl> otp;

  RGWCtlDef();
  ~RGWCtlDef();

  int init(RGWServices& svc);
};

struct RGWCtl {
  RGWCtlDef _ctl;

  struct _meta {
    RGWMetadataManager *mgr{nullptr};

    RGWMetadataHandler *bucket{nullptr};
    RGWMetadataHandler *bucket_instance{nullptr};
    RGWMetadataHandler *user{nullptr};
    RGWMetadataHandler *otp{nullptr};
  } meta;

  RGWUserCtl *user{nullptr};
  RGWBucketCtl *bucket{nullptr};
  RGWOTPCtl *otp{nullptr};

  int init(RGWServices& svc);
};

#endif
