// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_service.h"

#include "services/svc_finisher.h"
#include "services/svc_bucket.h"
#include "services/svc_cls.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_notify.h"
#include "services/svc_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw


RGWServices_Def::RGWServices_Def() = default;
RGWServices_Def::~RGWServices_Def()
{
  shutdown();
}

int RGWServices_Def::init(CephContext *cct,
			  bool have_cache,
                          bool raw)
{
  finisher = std::make_unique<RGWSI_Finisher>(cct);
  bucket = std::make_unique<RGWSI_Bucket>(cct);
  cls = std::make_unique<RGWSI_Cls>(cct);
  mdlog = std::make_unique<RGWSI_MDLog>(cct);
  meta = std::make_unique<RGWSI_Meta>(cct);
  meta_be_sobj = std::make_unique<RGWSI_MetaBackend_SObj>(cct);
  notify = std::make_unique<RGWSI_Notify>(cct);
  rados = std::make_unique<RGWSI_RADOS>(cct);
  zone = std::make_unique<RGWSI_Zone>(cct);
  zone_utils = std::make_unique<RGWSI_ZoneUtils>(cct);
  quota = std::make_unique<RGWSI_Quota>(cct);
  sync_modules = std::make_unique<RGWSI_SyncModules>(cct);
  sysobj = std::make_unique<RGWSI_SysObj>(cct);
  sysobj_core = std::make_unique<RGWSI_SysObj_Core>(cct);

  if (have_cache) {
    sysobj_cache = std::make_unique<RGWSI_SysObj_Cache>(cct);
  }

  vector<RGWSI_MetaBackend *> meta_bes{meta_be_sobj.get()};

  finisher->init();
  bucket->init(zone.get(), sysobj.get(), sysobj_cache.get(), meta.get(), sync_modules.get());
  cls->init(zone.get(), rados.get());
  mdlog->init(zone.get(), sysobj.get());
  meta->init(sysobj.get(), mdlog.get(), meta_bes);
  meta_be_sobj->init(sysobj.get(), mdlog.get());
  notify->init(zone.get(), rados.get(), finisher.get());
  rados->init();
  zone->init(sysobj.get(), rados.get(), sync_modules.get());
  zone_utils->init(rados.get(), zone.get());
  quota->init(zone.get());
  sync_modules->init(zone.get());
  sysobj_core->core_init(rados.get(), zone.get());
  if (have_cache) {
    sysobj_cache->init(rados.get(), zone.get(), notify.get());
    sysobj->init(rados.get(), sysobj_cache.get());
  } else {
    sysobj->init(rados.get(), sysobj_core.get());
  }

  can_shutdown = true;

  int r = finisher->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start finisher service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (!raw) {
    r = notify->start();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start notify service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = rados->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start rados service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (!raw) {
    r = zone->start();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start zone service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = sync_modules->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start sync modules service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = cls->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start cls service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_utils->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start zone_utils service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = quota->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start quota service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = sysobj_core->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start sysobj_core service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (have_cache) {
    r = sysobj_cache->start();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start sysobj_cache service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = sysobj->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start sysobj service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = mdlog->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start mdlog service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta_be_sobj->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start meta_be_sobj service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = meta->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start meta service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = bucket->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start bucket service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  /* cache or core services will be started by sysobj */

  return  0;
}

void RGWServices_Def::shutdown()
{
  if (!can_shutdown) {
    return;
  }

  if (has_shutdown) {
    return;
  }

  sysobj->shutdown();
  sysobj_core->shutdown();
  notify->shutdown();
  if (sysobj_cache) {
    sysobj_cache->shutdown();
  }
  quota->shutdown();
  zone_utils->shutdown();
  zone->shutdown();
  rados->shutdown();

  has_shutdown = true;

}


int RGWServices::do_init(CephContext *cct, bool have_cache, bool raw)
{
  int r = _svc.init(cct, have_cache, raw);
  if (r < 0) {
    return r;
  }

  finisher = _svc.finisher.get();
  bucket = _svc.bucket.get();
  cls = _svc.cls.get();
  mdlog = _svc.mdlog.get();
  meta = _svc.meta.get();
  meta_be = _svc.meta_be_sobj.get();
  notify = _svc.notify.get();
  rados = _svc.rados.get();
  zone = _svc.zone.get();
  zone_utils = _svc.zone_utils.get();
  quota = _svc.quota.get();
  sync_modules = _svc.sync_modules.get();
  sysobj = _svc.sysobj.get();
  cache = _svc.sysobj_cache.get();
  core = _svc.sysobj_core.get();

  return 0;
}

int RGWServiceInstance::start()
{
  if (start_state != StateInit) {
    return 0;
  }

  start_state = StateStarting;; /* setting started prior to do_start() on purpose so that circular
                                   references can call start() on each other */

  int r = do_start();
  if (r < 0) {
    return r;
  }

  start_state = StateStarted;

  return 0;
}
