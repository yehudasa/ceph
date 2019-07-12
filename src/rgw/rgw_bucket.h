// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>
#include <memory>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_metadata.h"

#include "rgw_string.h"

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/RWLock.h"
#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/ceph_time.h"
#include "common/optional_ref_default.h"

#include "rgw_formats.h"

#include "services/svc_bucket_types.h"

class RGWSI_Meta;
class RGWBucketMetadataHandler;
class RGWBucketInstanceMetadataHandler;
class RGWUserCtl;

extern int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *target_bucket_instance, int *shard_id);
extern int rgw_bucket_parse_bucket_key(CephContext *cct, const string& key,
                                       rgw_bucket* bucket, int *shard_id);

extern std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                              const std::string& bucket_name);
static inline void rgw_make_bucket_entry_name(const string& tenant_name,
                                              const string& bucket_name,
                                              std::string& bucket_entry) {
  bucket_entry = rgw_make_bucket_entry_name(tenant_name, bucket_name);
}

extern void rgw_parse_url_bucket(const string& bucket,
                                 const string& auth_tenant,
                                 string &tenant_name, string &bucket_name);

struct RGWBucketCompleteInfo {
  RGWBucketInfo info;
  map<string, bufferlist> attrs;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWBucketEntryMetadataObject : public RGWMetadataObject {
  RGWBucketEntryPoint ep;
  map<string, bufferlist> attrs;
public:
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, const obj_version& v, real_time m) : ep(_ep) {
    objv = v;
    mtime = m;
    set_pattrs (&attrs);
  }
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, const obj_version& v, real_time m, std::map<string, bufferlist>&& _attrs) :
    ep(_ep), attrs(std::move(_attrs)) {
    objv = v;
    mtime = m;
    set_pattrs (&attrs);
  }

  void dump(Formatter *f) const override {
    ep.dump(f);
  }

  RGWBucketEntryPoint& get_ep() {
    return ep;
  }

  map<string, bufferlist>& get_attrs() {
    return attrs;
  }
};

class RGWBucketInstanceMetadataObject : public RGWMetadataObject {
  RGWBucketCompleteInfo info;
public:
  RGWBucketInstanceMetadataObject() {}
  RGWBucketInstanceMetadataObject(RGWBucketCompleteInfo& i, const obj_version& v, real_time m) : info(i) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  void decode_json(JSONObj *obj) {
    info.decode_json(obj);
  }

  RGWBucketCompleteInfo& get_bci() {
    return info;
  }
  RGWBucketInfo& get_bucket_info() {
    return info.info;
  }
};

/**
 * Store a list of the user's buckets, with associated functinos.
 */
class RGWUserBuckets
{
  std::map<std::string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() = default;
  RGWUserBuckets(RGWUserBuckets&&) = default;

  RGWUserBuckets& operator=(const RGWUserBuckets&) = default;

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(buckets, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(buckets, bl);
  }
  /**
   * Check if the user owns a bucket by the given name.
   */
  bool owns(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  /**
   * Add a (created) bucket to the user's bucket list.
   */
  void add(const RGWBucketEnt& bucket) {
    buckets[bucket.bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(const string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  /**
   * Get the user's buckets as a map.
   */
  map<string, RGWBucketEnt>& get_buckets() { return buckets; }

  /**
   * Cleanup data structure
   */
  void clear() { buckets.clear(); }

  size_t count() { return buckets.size(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

class RGWBucketMetadataHandlerBase : public RGWMetadataHandler_GenericMetaBE {
public:
  virtual ~RGWBucketMetadataHandlerBase() {}
  virtual void init(RGWSI_Bucket *bucket_svc,
                    RGWBucketCtl *bucket_ctl) = 0;

};

class RGWBucketInstanceMetadataHandlerBase : public RGWMetadataHandler_GenericMetaBE {
public:
  virtual ~RGWBucketInstanceMetadataHandlerBase() {}
  virtual void init(RGWSI_Zone *zone_svc,
                    RGWSI_Bucket *bucket_svc,
                    RGWSI_BucketIndex *bi_svc) = 0;

};

class RGWBucketMetaHandlerAllocator {
public:
  static RGWBucketMetadataHandlerBase *alloc();
};

class RGWBucketInstanceMetaHandlerAllocator {
public:
  static RGWBucketInstanceMetadataHandlerBase *alloc();
};

class RGWArchiveBucketMetaHandlerAllocator {
public:
  static RGWBucketMetadataHandlerBase *alloc();
};

class RGWArchiveBucketInstanceMetaHandlerAllocator {
public:
  static RGWBucketInstanceMetadataHandlerBase *alloc();
};

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store,
                                 const rgw_user& user_id,
                                 RGWUserBuckets& buckets,
                                 const string& marker,
                                 const string& end_marker,
                                 uint64_t max,
                                 bool need_stats,
				 bool* is_truncated,
                                 uint64_t default_amount = 1000);

extern int rgw_remove_object(RGWRados *store, const RGWBucketInfo& bucket_info, const rgw_bucket& bucket, rgw_obj_key& key);
extern int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);
extern int rgw_remove_bucket_bypass_gc(RGWRados *store, rgw_bucket& bucket, int concurrent_max);

extern int rgw_object_get_attr(RGWRados* store, const RGWBucketInfo& bucket_info,
                               const rgw_obj& obj, const char* attr_name,
                               bufferlist& out_bl);

extern void check_bad_user_bucket_mapping(RGWRados *store, const rgw_user& user_id, bool fix);

struct RGWBucketAdminOpState {
  rgw_user uid;
  std::string display_name;
  std::string bucket_name;
  std::string bucket_id;
  std::string object_name;

  bool list_buckets;
  bool stat_buckets;
  bool check_objects;
  bool fix_index;
  bool delete_child_objects;
  bool bucket_stored;
  int max_aio = 0;

  rgw_bucket bucket;

  RGWQuotaInfo quota;

  void set_fetch_stats(bool value) { stat_buckets = value; }
  void set_check_objects(bool value) { check_objects = value; }
  void set_fix_index(bool value) { fix_index = value; }
  void set_delete_children(bool value) { delete_child_objects = value; }

  void set_max_aio(int value) { max_aio = value; }

  void set_user_id(const rgw_user& user_id) {
    if (!user_id.empty())
      uid = user_id;
  }
  void set_bucket_name(const std::string& bucket_str) {
    bucket_name = bucket_str; 
  }
  void set_object(std::string& object_str) {
    object_name = object_str;
  }
  void set_quota(RGWQuotaInfo& value) {
    quota = value;
  }


  rgw_user& get_user_id() { return uid; }
  std::string& get_user_display_name() { return display_name; }
  std::string& get_bucket_name() { return bucket_name; }
  std::string& get_object_name() { return object_name; }

  rgw_bucket& get_bucket() { return bucket; }
  void set_bucket(rgw_bucket& _bucket) {
    bucket = _bucket; 
    bucket_stored = true;
  }

  void set_bucket_id(const string& bi) {
    bucket_id = bi;
  }
  const string& get_bucket_id() { return bucket_id; }

  bool will_fetch_stats() { return stat_buckets; }
  bool will_fix_index() { return fix_index; }
  bool will_delete_children() { return delete_child_objects; }
  bool will_check_objects() { return check_objects; }
  bool is_user_op() { return !uid.empty(); }
  bool is_system_op() { return uid.empty(); }
  bool has_bucket_stored() { return bucket_stored; }
  int get_max_aio() { return max_aio; }

  RGWBucketAdminOpState() : list_buckets(false), stat_buckets(false), check_objects(false), 
                            fix_index(false), delete_child_objects(false),
                            bucket_stored(false)  {}
};

/*
 * A simple wrapper class for administrative bucket operations
 */

class RGWBucket
{
  RGWUserBuckets buckets;
  RGWRados *store;
  RGWAccessHandle handle;

  RGWUserInfo user_info;
  std::string tenant;
  std::string bucket_name;

  bool failure;

  RGWBucketInfo bucket_info;

public:
  RGWBucket() : store(NULL), handle(NULL), failure(false) {}
  int init(RGWRados *storage, RGWBucketAdminOpState& op_state);

  int check_bad_index_multipart(RGWBucketAdminOpState& op_state,
              RGWFormatterFlusher& flusher, std::string *err_msg = NULL);

  int check_object_index(RGWBucketAdminOpState& op_state,
                         RGWFormatterFlusher& flusher,
                         std::string *err_msg = NULL);

  int check_index(RGWBucketAdminOpState& op_state,
          map<RGWObjCategory, RGWStorageStats>& existing_stats,
          map<RGWObjCategory, RGWStorageStats>& calculated_stats,
          std::string *err_msg = NULL);

  int remove(RGWBucketAdminOpState& op_state, bool bypass_gc = false, bool keep_index_consistent = true, std::string *err_msg = NULL);
  int link(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int unlink(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int set_quota(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);

  int remove_object(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int policy_bl_to_stream(bufferlist& bl, ostream& o);
  int get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy);

  void clear_failure() { failure = false; }

  const RGWBucketInfo& get_bucket_info() const { return bucket_info; }
};

class RGWBucketAdminOp
{
public:
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy);
  static int dump_s3_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os);

  static int unlink(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int link(RGWRados *store, RGWBucketAdminOpState& op_state, string *err_msg = NULL);

  static int check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state, bool bypass_gc = false, bool keep_index_consistent = true);
  static int remove_object(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int info(RGWRados *store, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher);
  static int limit_check(RGWRados *store, RGWBucketAdminOpState& op_state,
			 const std::list<std::string>& user_ids,
			 RGWFormatterFlusher& flusher,
			 bool warnings_only = false);
  static int set_quota(RGWRados *store, RGWBucketAdminOpState& op_state);

  static int list_stale_instances(RGWRados *store, RGWBucketAdminOpState& op_state,
				  RGWFormatterFlusher& flusher);

  static int clear_stale_instances(RGWRados *store, RGWBucketAdminOpState& op_state,
				   RGWFormatterFlusher& flusher);
  static int fix_lc_shards(RGWRados *store, RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher);
  static int fix_obj_expiry(RGWRados *store, RGWBucketAdminOpState& op_state,
			    RGWFormatterFlusher& flusher, bool dry_run = false);
};


enum DataLogEntityType {
  ENTITY_TYPE_UNKNOWN = 0,
  ENTITY_TYPE_BUCKET = 1,
};

struct rgw_data_change {
  DataLogEntityType entity_type;
  string key;
  real_time timestamp;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    uint8_t t = (uint8_t)entity_type;
    encode(t, bl);
    encode(key, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     uint8_t t;
     decode(t, bl);
     entity_type = (DataLogEntityType)t;
     decode(key, bl);
     decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_data_change)

struct rgw_data_change_log_entry {
  string log_id;
  real_time log_timestamp;
  rgw_data_change entry;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(log_id, bl);
    encode(log_timestamp, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(log_id, bl);
     decode(log_timestamp, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_data_change_log_entry)

struct RGWDataChangesLogInfo {
  string marker;
  real_time last_update;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

namespace rgw {
struct BucketChangeObserver;
}

struct RGWDataChangesLogMarker {
  int shard;
  string marker;

  RGWDataChangesLogMarker() : shard(0) {}
};

class RGWDataChangesLog {
  CephContext *cct;
  rgw::BucketChangeObserver *observer = nullptr;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Cls *cls{nullptr};
  } svc;

  int num_shards;
  string *oids;

  Mutex lock;
  RWLock modified_lock;
  map<int, set<string> > modified_shards;

  std::atomic<bool> down_flag = { false };

  struct ChangeStatus {
    real_time cur_expiration;
    real_time cur_sent;
    bool pending;
    RefCountedCond *cond;
    Mutex *lock;

    ChangeStatus() : pending(false), cond(NULL) {
      lock = new Mutex("RGWDataChangesLog::ChangeStatus");
    }

    ~ChangeStatus() {
      delete lock;
    }
  };

  typedef std::shared_ptr<ChangeStatus> ChangeStatusPtr;

  lru_map<rgw_bucket_shard, ChangeStatusPtr> changes;

  map<rgw_bucket_shard, bool> cur_cycle;

  void _get_change(const rgw_bucket_shard& bs, ChangeStatusPtr& status);
  void register_renew(rgw_bucket_shard& bs);
  void update_renewed(rgw_bucket_shard& bs, real_time& expiration);

  class ChangesRenewThread : public Thread {
    CephContext *cct;
    RGWDataChangesLog *log;
    Mutex lock;
    Cond cond;

  public:
    ChangesRenewThread(CephContext *_cct, RGWDataChangesLog *_log) : cct(_cct), log(_log), lock("ChangesRenewThread::lock") {}
    void *entry() override;
    void stop();
  };

  ChangesRenewThread *renew_thread;

public:

  RGWDataChangesLog(RGWSI_Zone *zone_svc, RGWSI_Cls *cls_svc);
  ~RGWDataChangesLog();

  int choose_oid(const rgw_bucket_shard& bs);
  const std::string& get_oid(int shard_id) const { return oids[shard_id]; }
  int add_entry(const rgw_bucket& bucket, int shard_id);
  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  int renew_entries();
  int list_entries(int shard, const real_time& start_time, const real_time& end_time, int max_entries,
		   list<rgw_data_change_log_entry>& entries,
		   const string& marker,
		   string *out_marker,
		   bool *truncated);
  int trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                   const string& start_marker, const string& end_marker);
  int trim_entries(const real_time& start_time, const real_time& end_time,
                   const string& start_marker, const string& end_marker);
  int get_info(int shard_id, RGWDataChangesLogInfo *info);
#if 0
  int lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id);
  int unlock(int shard_id, string& zone_id, string& owner_id);
#endif

  using LogMarker = RGWDataChangesLogMarker;

  int list_entries(const real_time& start_time, const real_time& end_time, int max_entries,
               list<rgw_data_change_log_entry>& entries, LogMarker& marker, bool *ptruncated);

  void mark_modified(int shard_id, const rgw_bucket_shard& bs);
  void read_clear_modified(map<int, set<string> > &modified);

  void set_observer(rgw::BucketChangeObserver *observer) {
    this->observer = observer;
  }

  bool going_down();
};

class RGWBucketCtl
{
  CephContext *cct;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
  } svc;

  struct Ctl {
    RGWUserCtl *user{nullptr};
  } ctl;

  RGWBucketMetadataHandler *bm_handler;
  RGWBucketInstanceMetadataHandler *bmi_handler;

  RGWSI_Bucket_BE_Handler bucket_be_handler; /* bucket backend handler */
  RGWSI_BucketInstance_BE_Handler bi_be_handler; /* bucket instance backend handler */

  int call(std::function<int(RGWSI_Bucket_X_Ctx& ctx)> f);
  
public:
  RGWBucketCtl(RGWSI_Zone *zone_svc,
               RGWSI_Bucket *bucket_svc,
               RGWSI_BucketIndex *bi_svc);

  void init(RGWUserCtl *user_ctl,
            RGWBucketMetadataHandler *_bm_handler,
            RGWBucketInstanceMetadataHandler *_bmi_handler);

  struct Bucket {
    struct GetParams {
      RGWObjVersionTracker *objv_tracker{nullptr};
      real_time *mtime{nullptr};
      map<string, bufferlist> *attrs{nullptr};
      rgw_cache_entry_info *cache_info{nullptr};
      boost::optional<obj_version> refresh_version;

      GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      GetParams& set_mtime(ceph::real_time *_mtime) {
        mtime = _mtime;
        return *this;
      }

      GetParams& set_attrs(map<string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
        cache_info = _cache_info;
        return *this;
      }

      GetParams& set_refresh_version(const obj_version& _refresh_version) {
        refresh_version = _refresh_version;
        return *this;
      }
    };

    struct PutParams {
      RGWObjVersionTracker *objv_tracker{nullptr};
      ceph::real_time mtime;
      bool exclusive{false};
      map<string, bufferlist> *attrs{nullptr};

      PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      PutParams& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      PutParams& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      PutParams& set_attrs(map<string, bufferlist> *_attrs) {
        attrs = _attrs;
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
  };

  struct BucketInstance {
    struct GetParams {
      real_time *mtime{nullptr};
      map<string, bufferlist> *attrs{nullptr};
      rgw_cache_entry_info *cache_info{nullptr};
      boost::optional<obj_version> refresh_version;
      RGWObjVersionTracker *objv_tracker{nullptr};
      std::optional<RGWSI_MetaBackend_CtxParams> bectx_params;

      GetParams& set_mtime(ceph::real_time *_mtime) {
        mtime = _mtime;
        return *this;
      }

      GetParams& set_attrs(map<string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
        cache_info = _cache_info;
        return *this;
      }

      GetParams& set_refresh_version(const obj_version& _refresh_version) {
        refresh_version = _refresh_version;
        return *this;
      }

      GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      GetParams& set_bectx_params(const RGWSI_MetaBackend_CtxParams& _bectx_params) {
        bectx_params = _bectx_params;
        return *this;
      }
    };

    struct PutParams {
      std::optional<RGWBucketInfo *> orig_info; /* nullopt: orig_info was not fetched,
                                                   nullptr: orig_info was not found (new bucket instance */
      ceph::real_time mtime;
      bool exclusive{false};
      map<string, bufferlist> *attrs{nullptr};
      RGWObjVersionTracker *objv_tracker{nullptr};

      PutParams& set_orig_info(RGWBucketInfo *pinfo) {
        orig_info = pinfo;
        return *this;
      }

      PutParams& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      PutParams& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      PutParams& set_attrs(map<string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
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
  };

  /* bucket entrypoint */
  int read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                  RGWBucketEntryPoint *info,
                                  optional_yield y,
                                  ceph::optional_ref_default<RGWBucketCtl::Bucket::GetParams> params = std::nullopt);
  int store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                   RGWBucketEntryPoint& info,
                                   optional_yield y,
                                   ceph::optional_ref_default<RGWBucketCtl::Bucket::PutParams> params = std::nullopt);
  int remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                    optional_yield y,
                                    ceph::optional_ref_default<RGWBucketCtl::Bucket::RemoveParams> params = std::nullopt);

  /* bucket instance */
  int read_bucket_instance_info(const rgw_bucket& bucket,
                                  RGWBucketInfo *info,
                                  optional_yield y,
                                  ceph::optional_ref_default<RGWBucketCtl::BucketInstance::GetParams> params = std::nullopt);
  int store_bucket_instance_info(const rgw_bucket& bucket,
                                 RGWBucketInfo& info,
                                 optional_yield y,
                                 ceph::optional_ref_default<RGWBucketCtl::BucketInstance::PutParams> params = std::nullopt);
  int remove_bucket_instance_info(const rgw_bucket& bucket,
                                  optional_yield y,
                                  RGWBucketInfo& info,
                                  ceph::optional_ref_default<RGWBucketCtl::BucketInstance::RemoveParams> params = std::nullopt);

  /*
   * bucket_id may or may not be provided
   */
  int read_bucket_info(const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       optional_yield y,
                       ceph::optional_ref_default<RGWBucketCtl::BucketInstance::GetParams> _params = std::nullopt);



  int set_bucket_instance_attrs(RGWBucketInfo& bucket_info,
                                map<string, bufferlist>& attrs,
                                RGWObjVersionTracker *objv_tracker,
                                optional_yield y);

  /* user/bucket */
  int link_bucket(const rgw_user& user_id,
                  const rgw_bucket& bucket,
                  ceph::real_time creation_time,
                  bool update_entrypoint = true);

  int unlink_bucket(const rgw_user& user_id,
                    const rgw_bucket& bucket,
                    bool update_entrypoint = true);

  int read_buckets_stats(map<string, RGWBucketEnt>& m,
                         optional_yield y);

  int read_bucket_stats(const rgw_bucket& bucket,
                        RGWBucketEnt *result,
                        optional_yield y);

  /* quota related */
  int sync_user_stats(const rgw_user& user_id, const RGWBucketInfo& bucket_info);

private:
  int convert_old_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                              const rgw_bucket& bucket,
                              optional_yield y);

  int do_store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                    const rgw_bucket& bucket,
                                    RGWBucketInfo& info,
                                    optional_yield y,
                                    ceph::optional_ref_default<RGWBucketCtl::BucketInstance::PutParams> params);

  int do_store_linked_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                  RGWBucketInfo& info,
                                  RGWBucketInfo *orig_info,
                                  bool exclusive, real_time mtime,
                                  obj_version *pep_objv,
                                  map<string, bufferlist> *pattrs,
                                  bool create_entry_point);

  int do_link_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                     const rgw_user& user,
                     const rgw_bucket& bucket,
                     ceph::real_time creation_time,
                     bool update_entrypoint);

  int do_unlink_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                       const rgw_user& user_id,
                       const rgw_bucket& bucket,
                       bool update_entrypoint);

};


#endif
