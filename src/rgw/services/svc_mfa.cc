

#include "svc_mfa.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

#include "cls/otp/cls_otp_client.h"


#define dout_subsys ceph_subsys_rgw


int RGWSI_MFA::get_mfa_obj(const rgw_user& user, std::optional<RGWSI_RADOS::Obj> *obj) {
  string oid = get_mfa_oid(user);
  rgw_raw_obj o(zone_svc->get_zone_params().otp_pool, oid);

  obj->emplace(rados_svc->obj(o));
  int r = (*obj)->open();
  if (r < 0) {
    ldout(cct, 4) << "failed to open rados context for " << o << dendl;
    return r;
  }

  return 0;
}

int RGWSI_MFA::get_mfa_ref(const rgw_user& user, rgw_rados_ref *ref) {
  std::optional<RGWSI_RADOS::Obj> obj;
  int r = get_mfa_obj(user, &obj);
  if (r < 0) {
    return r;
  }
  *ref = obj->get_ref();
  return 0;
}

int RGWSI_MFA::check_mfa(const rgw_user& user, const string& otp_id, const string& pin, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  rados::cls::otp::otp_check_t result;

  r = rados::cls::otp::OTP::check(cct, ref.ioctx, ref.obj.oid, otp_id, pin, &result);
  if (r < 0)
    return r;

  ldout(cct, 20) << "OTP check, otp_id=" << otp_id << " result=" << (int)result.result << dendl;

  return (result.result == rados::cls::otp::OTP_CHECK_SUCCESS ? 0 : -EACCES);
}

void RGWSI_MFA::prepare_mfa_write(librados::ObjectWriteOperation *op,
                                 RGWObjVersionTracker *objv_tracker,
                                 const ceph::real_time& mtime)
{
  RGWObjVersionTracker ot;

  if (objv_tracker) {
    ot = *objv_tracker;
  }

  if (ot.write_version.tag.empty()) {
    if (ot.read_version.tag.empty()) {
      ot.generate_new_write_ver(cct);
    } else {
      ot.write_version = ot.read_version;
      ot.write_version.ver++;
    }
  }

  ot.prepare_op_for_write(op);
  struct timespec mtime_ts = real_clock::to_timespec(mtime);
  op->mtime2(&mtime_ts);
}

int RGWSI_MFA::create_mfa(const rgw_user& user, const rados::cls::otp::otp_info_t& config,
                         RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime, optional_yield y)
{
  std::optional<RGWSI_RADOS::Obj> obj;
  int r = get_mfa_obj(user, &obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::create(&op, config);
  r = obj->operate(&op, y);
  if (r < 0) {
    ldout(cct, 20) << "OTP create, otp_id=" << config.id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_MFA::remove_mfa(const rgw_user& user, const string& id,
                         RGWObjVersionTracker *objv_tracker,
                         const ceph::real_time& mtime,
                         optional_yield y)
{
  std::optional<RGWSI_RADOS::Obj> obj;
  int r = get_mfa_obj(user, &obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::remove(&op, id);
  r = obj->operate(&op, y);
  if (r < 0) {
    ldout(cct, 20) << "OTP remove, otp_id=" << id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_MFA::get_mfa(const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result,
                       optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get(nullptr, ref.ioctx, ref.obj.oid, id, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_MFA::list_mfa(const rgw_user& user, list<rados::cls::otp::otp_info_t> *result,
                        optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_all(nullptr, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_MFA::otp_get_current_time(const rgw_user& user, ceph::real_time *result,
                                    optional_yield y)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_current_time(ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_MFA::set_mfa(const string& oid, const list<rados::cls::otp::otp_info_t>& entries,
                      bool reset_obj, RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime,
                      optional_yield y)
{
  rgw_raw_obj o(zone_svc->get_zone_params().otp_pool, oid);
  auto obj = rados_svc->obj(o);
  int r = obj.open();
  if (r < 0) {
    ldout(cct, 4) << "failed to open rados context for " << o << dendl;
    return r;
  }
  librados::ObjectWriteOperation op;
  if (reset_obj) {
    op.remove();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.create(false);
  }
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::set(&op, entries);
  r = obj.operate(&op, y);
  if (r < 0) {
    ldout(cct, 20) << "OTP set entries.size()=" << entries.size() << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_MFA::list_mfa(const string& oid, list<rados::cls::otp::otp_info_t> *result,
                       RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime,
                       optional_yield y)
{
  rgw_raw_obj o(zone_svc->get_zone_params().otp_pool, oid);
  auto obj = rados_svc->obj(o);
  int r = obj.open();
  if (r < 0) {
    ldout(cct, 4) << "failed to open rados context for " << o << dendl;
    return r;
  }
  auto& ref = obj.get_ref();
  librados::ObjectReadOperation op;
  struct timespec mtime_ts;
  if (pmtime) {
    op.stat2(nullptr, &mtime_ts, nullptr);
  }
  objv_tracker->prepare_op_for_read(&op);
  r = rados::cls::otp::OTP::get_all(&op, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }
  if (pmtime) {
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  }

  return 0;
}

