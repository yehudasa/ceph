// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_OTP_H
#define CEPH_RGW_OTP_H

#include "common/optional_ref_default.h"

#include "cls/otp/cls_otp_types.h"
#include "services/svc_meta_be_otp.h"

#include "rgw_basic_types.h"


class RGWObjVersionTracker;
class RGWMetadataHandler;
class RGWOTPMetadataHandler;
class RGWSI_Zone;
class RGWSI_OTP;
class RGWSI_MetaBackend;

class RGWOTPMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

struct RGWOTPInfo {
  rgw_user uid;
  otp_devices_list_t devices;
};


class RGWOTPCtl
{
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_OTP *otp{nullptr};
  } svc;

  RGWOTPMetadataHandler *meta_handler;
  RGWSI_MetaBackend_Handler *be_handler;
  
public:
  RGWOTPCtl(RGWSI_Zone *zone_svc,
             RGWSI_OTP *otp_svc,
             RGWOTPMetadataHandler *_meta_handler);

  struct GetParams {
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time *mtime{nullptr};

    GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    GetParams& set_mtime(ceph::real_time *_mtime) {
      mtime = _mtime;
      return *this;
    }
  };

  struct PutParams {
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time mtime;

    PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    PutParams& set_mtime(const ceph::real_time& _mtime) {
      mtime = _mtime;
      return *this;
    }
  };

  struct RemoveParams {
    RGWObjVersionTracker *objv_tracker{nullptr};

    RemoveParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }
  };

  int read_all(const rgw_user& uid, RGWOTPInfo *info, ceph::optional_ref_default<GetParams> params);
  int store_all(const RGWOTPInfo& info, ceph::optional_ref_default<PutParams> params);
  int remove_all(const rgw_user& user, ceph::optional_ref_default<RemoveParams> params);
};

#endif

