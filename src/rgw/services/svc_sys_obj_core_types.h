

#pragma once


#include "rgw/rgw_service.h"

#include "svc_rados.h"
#include "svc_sys_obj_types.h"



struct RGWSI_SysObj_Core_GetObjState : public RGWSI_SysObj_Obj_GetObjState {
  RGWSI_RADOS::Obj rados_obj;
  bool has_rados_obj{false};
  uint64_t last_ver{0};

  RGWSI_SysObj_Core_GetObjState() {}

  int get_rados_obj(RGWSI_RADOS *rados_svc,
                    RGWSI_Zone *zone_svc,
                    const rgw_raw_obj& obj,
                    RGWSI_RADOS::Obj **pobj);
};

struct RGWSI_SysObj_Core_PoolListImplInfo : public RGWSI_SysObj_Pool_ListInfo {
  RGWSI_RADOS::Pool pool;
  RGWSI_RADOS::Pool::List op;
  RGWAccessListFilterPrefix filter;

  RGWSI_SysObj_Core_PoolListImplInfo(const string& prefix) : op(pool.op()), filter(prefix) {}
};

struct RGWSysObjState {
  rgw_raw_obj obj;
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0};
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  uint64_t pg_ver{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;
  RGWSysObjState() {}
  RGWSysObjState(const RGWSysObjState& rhs) : obj (rhs.obj) {
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    pg_ver = rhs.pg_ver;
    objv_tracker = rhs.objv_tracker;
  }
};


class RGWSysObjectCtxBase {
  std::map<rgw_raw_obj, RGWSysObjState> objs_state;
  RWLock lock;

public:
  explicit RGWSysObjectCtxBase() : lock("RGWSysObjectCtxBase") {}

  RGWSysObjectCtxBase(const RGWSysObjectCtxBase& rhs) : objs_state(rhs.objs_state),
                                                  lock("RGWSysObjectCtxBase") {}
  RGWSysObjectCtxBase(const RGWSysObjectCtxBase&& rhs) : objs_state(std::move(rhs.objs_state)),
                                                   lock("RGWSysObjectCtxBase") {}

  RGWSysObjState *get_state(const rgw_raw_obj& obj) {
    RGWSysObjState *result;
    std::map<rgw_raw_obj, RGWSysObjState>::iterator iter;
    lock.get_read();
    assert (!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  }

  void set_prefetch_data(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(const rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    objs_state.erase(iter);
  }
};

