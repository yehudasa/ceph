#include "svc_sys_obj.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

int RGWS_SysObj::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_SysObj(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_SysObj::get_deps()
{
  RGWServiceInstance::dependency dep1 = { .name = "rados",
                                          .conf = "{}" };
  RGWServiceInstance::dependency dep2 = { .name = "zone",
                                          .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["rados_dep"] = dep1;
  deps["zone_dep"] = dep2;
  return deps;
}

int RGWSI_SysObj::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  rados_svc = static_pointer_cast<RGWSI_RADOS>(dep_refs["rados_dep"]);
  assert(rados_svc);

  zone_svc = static_pointer_cast<RGWSI_Zone>(dep_refs["zone_dep"]);
  assert(zone_svc);

  return 0;
}

int RGWSI_SysObj::get_rados_obj(RGWSI_Zone *zone_svc,
                                rgw_raw_obj& obj,
                                RGWSI_RADOS::Obj *pobj)
{
  zone_svc->canonicalize_raw_obj(&obj);

  *pobj = std::move(rados_svc->obj(obj));
  int r = pobj->open();
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_SysObj::get_system_obj_state_impl(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker)
{
  if (obj.empty()) {
    return -EINVAL;
  }

  RGWSysObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    return 0;
  }

  s->obj = obj;

  int r = raw_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : nullptr), objv_tracker);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = real_time();
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

  if (s->obj_tag.length())
    ldout(cct, 20) << "get_system_obj_state: setting s->obj_tag to "
                   << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;

  return 0;
}

int RGWSI_SysObj::get_system_obj_state(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker)
{
  int ret;

  do {
    ret = get_system_obj_state_impl(rctx, obj, state, objv_tracker);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWSI_SysObj::raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                           map<string, bufferlist> *attrs, bufferlist *first_chunk,
                           RGWObjVersionTracker *objv_tracker)
{
  RGWSI_RADOS::Obj rados_obj;
  int r = get_rados_obj(zone_svc.get(), obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  struct timespec mtime_ts;

  librados::ObjectReadOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }
  if (attrs) {
    op.getxattrs(&unfiltered_attrset, nullptr);
  }
  if (psize || pmtime) {
    op.stat2(&size, &mtime_ts, nullptr);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, nullptr);
  }
  bufferlist outbl;
  r = rados_obj.operate(&op, &outbl);

  if (epoch) {
    *epoch = rados_obj.get_last_version();
  }

  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  if (attrs) {
    rgw_filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  return 0;
}

int RGWSI_SysObj::stat(RGWSysObjectCtx& obj_ctx,
                       RGWSI_SysObj::Obj::ROp::GetObjState& state,
                       rgw_raw_obj& obj,
                       map<string, bufferlist> *attrs,
                       real_time *lastmod,
                       uint64_t *obj_size,
                       RGWObjVersionTracker *objv_tracker)
{
  RGWSysObjState *astate = nullptr;

  int r = get_system_obj_state(&obj_ctx, obj, &astate, objv_tracker);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      map<string, bufferlist>::iterator iter;
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return 0;
}

int RGWSI_SysObj::read(RGWSysObjectCtx& obj_ctx,
                       Obj::ROp::GetObjState& read_state,
                       RGWObjVersionTracker *objv_tracker,
                       rgw_raw_obj& obj,
                       bufferlist *bl, off_t ofs, off_t end,
                       map<string, bufferlist> *attrs,
                       boost::optional<obj_version>)
{
  uint64_t len;
  librados::ObjectReadOperation op;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }

  ldout(cct, 20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len, bl, nullptr);

  if (attrs) {
    op.getxattrs(attrs, nullptr);
  }

  RGWSI_RADOS::Obj rados_obj;
  int r = get_rados_obj(zone_svc.get(), obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }
  r = rados_obj.operate(&op, nullptr);
  if (r < 0) {
    ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;
    return r;
  }
  ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;

  uint64_t op_ver = rados_obj.get_last_version();

  if (read_state.last_ver > 0 &&
      read_state.last_ver != op_ver) {
    ldout(cct, 5) << "raced with an object write, abort" << dendl;
    return -ECANCELED;
  }

  read_state.last_ver = op_ver;

  return bl->length();
}

/**
 * Get an attribute for a system object.
 * obj: the object to get attr
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWSI_SysObj::get_attr(rgw_raw_obj& obj,
                           const char *name,
                           bufferlist *dest)
{
  RGWSI_RADOS::Obj rados_obj;
  int r = get_rados_obj(zone_svc.get(), obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  librados::ObjectReadOperation op;

  int rval;
  op.getxattr(name, dest, &rval);
  
  r = rados_obj.operate(&op, nullptr);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_SysObj::remove(RGWSysObjectCtx& obj_ctx,
                         RGWObjVersionTracker *objv_tracker,
                         rgw_raw_obj& obj)
{
  RGWSI_RADOS::Obj rados_obj;
  int r = get_rados_obj(zone_svc.get(), obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  librados::ObjectWriteOperation op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  op.remove();
  r = rados_obj.operate(&op);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_SysObj::write(rgw_raw_obj& obj,
                        real_time *pmtime,
                        map<std::string, bufferlist>& attrs,
                        bool exclusive,
                        const bufferlist& data,
                        RGWObjVersionTracker *objv_tracker,
                        real_time set_mtime)
{
  RGWSI_RADOS::Obj rados_obj;
  int r = get_rados_obj(zone_svc.get(), obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  librados::ObjectWriteOperation op;

  if (exclusive) {
    op.create(true); // exclusive create
  } else {
    op.remove();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.create(false);
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  if (real_clock::is_zero(set_mtime)) {
    set_mtime = real_clock::now();
  }

  struct timespec mtime_ts = real_clock::to_timespec(set_mtime);
  op.mtime2(&mtime_ts);
  op.write_full(data);

  bufferlist acl_bl;

  for (map<string, bufferlist>::iterator iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);
  }

  r = rados_obj.operate(&op);
  if (r < 0) {
    return r;
  }

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  if (pmtime) {
    *pmtime = set_mtime;
  }

  return 0;
}


void RGWSI_SysObj::Obj::invalidate_state()
{
  ctx.invalidate(obj);
}

int RGWSI_SysObj::Obj::ROp::GetObjState::get_rados_obj(RGWSI_RADOS *rados_svc,
                                                       RGWSI_Zone *zone_svc,
                                                       rgw_raw_obj& obj,
                                                       RGWSI_RADOS::Obj **pobj)
{
  if (!has_rados_obj) {
    zone_svc->canonicalize_raw_obj(&obj);

    rados_obj = rados_svc->obj(obj);
    int r = rados_obj.open();
    if (r < 0) {
      return r;
    }
    has_rados_obj = true;
  }
  *pobj = &rados_obj;
  return 0;
}

int RGWSI_SysObj::Obj::ROp::stat()
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->stat(source.get_ctx(), state, obj, attrs,
                   lastmod, obj_size,
                   objv_tracker);
}


int RGWSI_SysObj::Obj::ROp::read(int64_t ofs, int64_t end, bufferlist *bl)
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(source.get_ctx(), state,
                   objv_tracker,
                   obj, bl, ofs, end,
                   attrs,
                   refresh_version);
}

int RGWSI_SysObj::Obj::ROp::get_attr(const char *name, bufferlist *dest)
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(obj, name, dest);
}

int RGWSI_SysObj::Obj::WOp::remove()
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->remove(source.get_ctx(),
                     objv_tracker,
                     obj);
}

int RGWSI_SysObj::Obj::WOp::write(bufferlist& bl)
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write(obj, pmtime, attrs, exclusive,
                    bl, objv_tracker, mtime);
}

int RGWSI_SysObj::Pool::Op::list_prefixed_objs(const string& prefix, list<string> *result)
{
  bool is_truncated;

  auto rados_svc = source.get_rados_svc();
  auto rados_pool = rados_svc->pool(source.pool);

  auto op = rados_pool.op();

  RGWAccessListFilterPrefix filter(prefix);

  int r = op.init(string(), &filter);
  if (r < 0) {
    return r;
  }

  do {
    list<string> oids;
#define MAX_OBJS_DEFAULT 1000
    int r = op.get_next(MAX_OBJS_DEFAULT, &oids, &is_truncated);
    if (r < 0) {
      return r;
    }
    for (auto& val : oids) {
      if (val.size() > prefix.size()) {
        result->push_back(val.substr(prefix.size()));
      }
    }
  } while (is_truncated);

  return 0;
}
