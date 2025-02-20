// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"

#include "rgw_bucket_snap_types.h"


struct rgw_bucket_snap_removal_state {
  rgw_bucket_snap_id snap_id;

  enum RemovalStatus {
    INIT = 0,
    PROCESSING = 1,
    COMPLETE = 2,
  } status = INIT;

  std::string marker;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(snap_id, bl);
    encode((uint16_t)status, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(snap_id, bl);
    uint16_t status_c;
    decode(status_c, bl);
    status = (RemovalStatus)status_c;
    decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_removal_state)

/*
 *  class RGWBucketSnapMgr
 *
 *  Operations to the bucket snap manager are first stored in operation log. These operations should
 *  be idempotent: create a snapshot, remove a snapshot, etc.
 *  The log itself is read on initialization and is reflected so that the object applies all the
 *  relevant changes. However, log is not cleared until the bucket snapshots worker processes
 *  it and clears the relevant entries (e.g., it scheduled snapshot cleanup).
 */
class RGWBucketSnapMgr
{
  bool enabled = false;
  rgw_bucket_snap_id cur_snap{rgw_bucket_snap_id::SNAP_MIN};

  std::map<rgw_bucket_snap_id, rgw_bucket_snap> snaps;

  std::map<std::string, rgw_bucket_snap_id> names_to_ids;

  std::map<rgw_bucket_snap_id, rgw_bucket_snap_removal_state> removed_snaps;

  struct create_snap_op_args {
    rgw_bucket_snap snap;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(snap, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(snap, bl);
      DECODE_FINISH(bl);
    }
  };

  struct rm_snap_op_args {
    rgw_bucket_snap_id snap_id;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(snap_id, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(snap_id, bl);
      DECODE_FINISH(bl);
    }
  };

  struct OperationLogEntry {
    enum Type {
      NO_OP       = 0,
      CREATE_SNAP = 1,
      REMOVE_SNAP = 2,
    } op_type;

    bufferlist data;
  };

  std::vector<OperationLogEntry> ops_log;

  void do_create_snap(const rgw_bucket_snap& snap);
  void do_rm_snap(const rgw_bucket_snap_id& snap_id);

  int reflect_log_entry(const OperationLogEntry& entry);
  int reflect_log();
public:
  RGWBucketSnapMgr();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(cur_snap, bl);
    encode(snaps, bl);
    encode(names_to_ids, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(cur_snap, bl);
    decode(snaps, bl);
    decode(names_to_ids, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  rgw_bucket_snap_id get_cur_snap_id() const {
    return cur_snap;
  }

  int log_create_snap(const rgw_bucket_snap_info& info);
  int log_rm_snap(const rgw_bucket_snap_id& snap_id);

  const std::map<rgw_bucket_snap_id, rgw_bucket_snap>& get_snaps() const {
    return snaps;
  }

  bool is_enabled() const {
    return enabled;
  }

  void set_enabled(bool flag) {
    enabled = flag;

    if (enabled) {
      ++cur_snap;
    }
  }

};
WRITE_CLASS_ENCODER(RGWBucketSnapMgr)
