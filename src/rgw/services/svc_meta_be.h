
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
#include "rgw/rgw_mdlog_types.h"

class RGWMetadataLogData;

class RGWSI_MDLog;
class RGWSI_Meta;
class RGWObjVersionTracker;
class RGWSI_MetaBackend_Handler;

class RGWSI_MetaBackend : public RGWServiceInstance
{
  friend class RGWSI_Meta;
public:
  class Module;
  class Context;
protected:
  RGWSI_MDLog *mdlog_svc{nullptr};

  void base_init(RGWSI_MDLog *_mdlog_svc) {
    mdlog_svc = _mdlog_svc;
  }

  int prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime,
                     RGWObjVersionTracker *objv_tracker,
                     RGWMDLogSyncType sync_mode);

  virtual int mutate(Context *ctx,
                     const std::string& key,
                     const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
                     RGWMDLogStatus op_type,
                     RGWMDLogSyncType sync_mode,
                     std::function<int()> f,
                     bool generic_prepare);

  virtual int pre_modify(Context *ctx,
                         const std::string& key,
                         RGWMetadataLogData& log_data,
                         RGWObjVersionTracker *objv_tracker,
                         RGWMDLogStatus op_type);
  virtual int post_modify(Context *ctx,
                          const std::string& key,
                          RGWMetadataLogData& log_data,
                          RGWObjVersionTracker *objv_tracker, int ret);
public:
  class Module {
    /*
     * Backend specialization module
     */
  public:
    virtual ~Module() = 0;
  };

  using ModuleRef = std::shared_ptr<Module>;

  struct Context { /*
                    * A single metadata operation context. Will be holding info about
                    * backend and operation itself; operation might span multiple backend
                    * calls.
                    */
    virtual ~Context() = 0;

    virtual void init(RGWSI_MetaBackend_Handler *h) = 0;
  };

  virtual Context *alloc_ctx() = 0;

  struct PutParams {
    ceph::real_time mtime;

    PutParams() {}
    PutParams(const ceph::real_time& _mtime) : mtime(_mtime) {}
    virtual ~PutParams() = 0;
  };

  struct GetParams {
    GetParams() {}
    GetParams(ceph::real_time *_pmtime) : pmtime(_pmtime) {}
    virtual ~GetParams();

    ceph::real_time *pmtime{nullptr};
  };

  struct RemoveParams {
    virtual ~RemoveParams() = 0;

    ceph::real_time mtime;
  };

  enum Type {
    MDBE_SOBJ = 0,
    MDBE_OTP  = 1,
  };

  RGWSI_MetaBackend(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_MetaBackend() {}

  virtual Type get_type() = 0;

  virtual RGWSI_MetaBackend_Handler *alloc_be_handler() = 0;
  virtual GetParams *alloc_default_get_params(ceph::real_time *pmtime) = 0;

  /* these should be implemented by backends */
  virtual int get_entry(RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::GetParams& params,
                        RGWObjVersionTracker *objv_tracker) = 0;
  virtual int put_entry(RGWSI_MetaBackend::Context *ctx,
                        const std::string& key,
                        RGWSI_MetaBackend::PutParams& params,
                        RGWObjVersionTracker *objv_tracker) = 0;
  virtual int remove_entry(Context *ctx,
                           const std::string& key,
                           RGWSI_MetaBackend::RemoveParams& params,
                           RGWObjVersionTracker *objv_tracker) = 0;

  virtual int list_init(RGWSI_MetaBackend::Context *ctx, const string& marker) = 0;
  virtual int list_next(RGWSI_MetaBackend::Context *ctx,
                        int max, list<string>& keys,
                        bool *truncated)  = 0;
  virtual int list_get_marker(RGWSI_MetaBackend::Context *ctx,
                              string *marker) = 0;

  virtual int call(std::function<int(RGWSI_MetaBackend::Context *)> f) = 0;

  /* higher level */
  virtual int get(Context *ctx,
                  const std::string& key,
                  GetParams &params,
                  RGWObjVersionTracker *objv_tracker);

  virtual int put(Context *ctx,
                  const std::string& key,
                  PutParams& params,
                  RGWObjVersionTracker *objv_tracker,
                  RGWMDLogSyncType sync_mode);

  virtual int remove(Context *ctx,
                     const std::string& key,
                     RemoveParams& params,
                     RGWObjVersionTracker *objv_tracker,
                     RGWMDLogSyncType sync_mode);

};

class RGWSI_MetaBackend_Handler {
  RGWSI_MetaBackend *be{nullptr};

public:
  class Op {
    friend class RGWSI_MetaBackend_Handler;

    RGWSI_MetaBackend *be;
    RGWSI_MetaBackend::Context *be_ctx;

    Op(RGWSI_MetaBackend *_be,
       RGWSI_MetaBackend::Context *_ctx) : be(_be), be_ctx(_ctx) {}

  public:
    RGWSI_MetaBackend::Context *ctx() {
      return be_ctx;
    }

    int get(const std::string& key,
            RGWSI_MetaBackend::GetParams &params,
            RGWObjVersionTracker *objv_tracker) {
      return be->get(be_ctx, key, params, objv_tracker);
    }

    int put(const std::string& key,
            RGWSI_MetaBackend::PutParams& params,
            RGWObjVersionTracker *objv_tracker,
            RGWMDLogSyncType sync_mode) {
      return be->put(be_ctx, key, params, objv_tracker, sync_mode);
    }

    int remove(const std::string& key,
               RGWSI_MetaBackend::RemoveParams& params,
               RGWObjVersionTracker *objv_tracker,
               RGWMDLogSyncType sync_mode) {
      return be->remove(be_ctx, key, params, objv_tracker, sync_mode);
    }

    int list_init(const string& marker) {
      return be->list_init(be_ctx, marker);
    }
    int list_next(int max, list<string>& keys,
                  bool *truncated) {
      return be->list_next(max, keys, truncated);
    }
    int list_get_marker(string *marker) {
      return be->list_get_marker(marker);
    }
  };

  class Op_ManagedCtx : public Op {
    std::unique_ptr<RGWSI_MetaBackend::Context> pctx;
  public:
    Op_ManagedCtx(RGWSI_MetaBackend *_be);
  };

  RGWSI_MetaBackend_Handler(RGWSI_MetaBackend *_be) : be(_be) {}
  virtual ~RGWSI_MetaBackend_Handler() {}

  virtual int call(std::function<int(Op *)> f);
  virtual Op *alloc_op();
};

