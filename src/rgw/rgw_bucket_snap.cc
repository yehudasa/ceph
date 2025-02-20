#include "rgw_bucket_snap.h"
#include "common/ceph_json.h"

#define dout_subsys ceph_subsys_rgw


void rgw_bucket_snap_info::dump(Formatter *f) const {
  encode_json("name", name, f);
  encode_json("description", description, f);
  encode_json("creation_time", creation_time, f);
}

void rgw_bucket_snap::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("info", info, f);
}

void rgw_bucket_snap_removal_state::dump(Formatter *f) const {
  encode_json("snap_id", snap_id, f);
  std::string status_str;
  switch (status) {
    case INIT:
      status_str = "init";
      break;
    case PROCESSING:
      status_str = "processing";
      break;
    case COMPLETE:
      status_str = "complete";
      break;
    default:
      status_str = "unknown";
  }
  encode_json("status", status_str, f);
  encode_json("marker", marker, f);
}

RGWBucketSnapMgr::RGWBucketSnapMgr() {
  reflect_log(); /* best effort */
}

void RGWBucketSnapMgr::dump(Formatter *f) const {
  encode_json("enabled", enabled, f);
  encode_json("cur_snap", cur_snap, f);
  encode_json("snaps", snaps, f);
  encode_json("names_to_ids", names_to_ids, f);
}

void RGWBucketSnapMgr::do_create_snap(const rgw_bucket_snap& snap)
{
  if (snap.id > cur_snap) {
    cur_snap = snap.id;
  }
  snaps[snap.id] = snap;
  names_to_ids[snap.info.name] = snap.id;
}

void RGWBucketSnapMgr::do_rm_snap(const rgw_bucket_snap_id& snap_id)
{
  snaps.erase(snap_id);

  auto iter = removed_snaps.find(snap_id);
  if (iter == removed_snaps.end()) {
    removed_snaps[snap_id] = rgw_bucket_snap_removal_state();
  }
}

int RGWBucketSnapMgr::log_create_snap(const rgw_bucket_snap_info& info)
{
  auto iter = names_to_ids.find(info.name);
  if (iter != names_to_ids.end()) {
    return -EEXIST;
  }

  rgw_bucket_snap snap;
  snap.id = cur_snap + 1;
  snap.info = info;

  OperationLogEntry entry;
  entry.op_type = OperationLogEntry::CREATE_SNAP;

  create_snap_op_args args;
  args.snap = snap;
  args.encode(entry.data);

  ops_log.push_back(entry);

  return 0;
}

int RGWBucketSnapMgr::log_rm_snap(const rgw_bucket_snap_id& snap_id)
{
  auto iter = snaps.find(snap_id);
  if (iter == snaps.end()) {
    /* does not exist, nothing to do */
    return -ENOENT;
  }

  OperationLogEntry entry;
  entry.op_type = OperationLogEntry::REMOVE_SNAP;

  rm_snap_op_args args;
  args.snap_id = snap_id;
  args.encode(entry.data);

  ops_log.push_back(entry);

  return 0;
}

int RGWBucketSnapMgr::reflect_log_entry(const OperationLogEntry& entry)
{
  auto bliter = entry.data.cbegin();
  try {
    switch(entry.op_type) {
      case OperationLogEntry::NO_OP:
        break;
      case OperationLogEntry::CREATE_SNAP:
        {
          create_snap_op_args args;
          args.decode(bliter);
          do_create_snap(args.snap);
          return 0;
        }
      case OperationLogEntry::REMOVE_SNAP:
        {
          rm_snap_op_args args;
          args.decode(bliter);
          do_rm_snap(args.snap_id);
          return 0;
        }
        break;
      default:
        /* unknown operation, we failed */
        return -EINVAL;
    }
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

int RGWBucketSnapMgr::reflect_log()
{
  for (auto& entry : ops_log) {
    int r = reflect_log_entry(entry);
    if (r < 0)  {
      return r;
    }
  }

  return 0;
}
