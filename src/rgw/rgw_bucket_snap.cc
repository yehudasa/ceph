#include "rgw_bucket_snap.h"
#include "common/ceph_json.h"


void rgw_bucket_snap_info::dump(Formatter *f) const {
  encode_json("name", name, f);
  encode_json("description", description, f);
  encode_json("creation_time", creation_time, f);
}

void rgw_bucket_snap::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("info", info, f);
}

void rgw_bucket_snap_revert_info::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("revert_to_id", revert_to_id, f);
  encode_json("description", description, f);
  encode_json("creation_time", creation_time, f);
}


RGWBucketSnapMgr::RGWBucketSnapMgr() {}

void RGWBucketSnapMgr::dump(Formatter *f) const {
  encode_json("enabled", enabled, f);
  encode_json("cur_snap", cur_snap, f);
  encode_json("snaps", snaps, f);
  encode_json("names_to_ids", names_to_ids, f);
  encode_json("revert_snaps", revert_snaps, f);
}

int RGWBucketSnapMgr::create_snap(const rgw_bucket_snap_info& info, rgw_bucket_snap_id *snap_id)
{
  auto iter = names_to_ids.find(info.name);
  if (iter != names_to_ids.end()) {
    return -EEXIST;
  }

  rgw_bucket_snap snap;
  snap.id = cur_snap++;
  snap.info = info;

  if (snap_id) {
    *snap_id = snap.id;
  }

  snaps[snap.id] = snap;
  names_to_ids[info.name] = snap.id;

  return 0;
}

rgw_bucket_snap_id RGWBucketSnapMgr::effective_snap_id(rgw_bucket_snap_id snap_id)
{
  if (revert_snaps.empty()) {
    return snap_id;
  }

  auto iter = revert_snaps.lower_bound(snap_id);
  if (iter == revert_snaps.end()) {
    return snap_id;
  }

  auto& entry = iter->second;
  if (entry.revert_to_id >= snap_id) {
    return snap_id;
  }

  return entry.revert_to_id;
}

int RGWBucketSnapMgr::set_revert(rgw_bucket_snap_id from, rgw_bucket_snap_id to,
                                 const std::string& description,
                                 const ceph::real_time& creation_time)
{
  if (from <= to) {
    /* can only revert backwards */
    return -EINVAL;
  }

  rgw_bucket_snap_revert_info entry;
  entry.id = from;
  entry.revert_to_id = to;
  entry.description = description;
  entry.creation_time = creation_time;

  revert_snaps[entry.id] = entry;

  return 0;
}
