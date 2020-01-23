// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DATA_SYNC_H
#define CEPH_RGW_DATA_SYNC_H

#include "include/encoding.h"

#include "common/RWLock.h"
#include "common/ceph_json.h"

#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "rgw_sal.h"

#include "rgw_sync_module.h"
#include "rgw_sync_trace.h"

class JSONObj;
struct rgw_sync_bucket_pipe;

struct rgw_bucket_sync_pair_info {
  rgw_bucket_shard source_bs;
  rgw_bucket_shard dest_bs;
  string source_prefix;
  string dest_prefix;
};

inline ostream& operator<<(ostream& out, const rgw_bucket_sync_pair_info& p) {
  if (p.source_bs.bucket == p.dest_bs.bucket &&
      p.source_prefix == p.dest_prefix) {
    return out << p.source_bs;
  }

  out << p.source_bs;

  if (!p.source_prefix.empty()) {
    out << "/" << p.source_prefix;
  }

  out << " -> " << p.dest_bs.bucket;

  if (!p.dest_prefix.empty()) {
    out << "/" << p.dest_prefix;
  }

  return out;
}

struct rgw_bucket_sync_pipe {
  rgw_bucket_sync_pair_info info;
  RGWBucketInfo source_bucket_info;
  RGWBucketInfo dest_bucket_info;
};

inline ostream& operator<<(ostream& out, const rgw_bucket_sync_pipe& p) {
  return out << p.info;
}

struct rgw_datalog_info {
  uint32_t num_shards;

  rgw_datalog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

struct rgw_data_sync_info {
  enum SyncState {
    StateInit = 0,
    StateBuildingFullSyncMaps = 1,
    StateSync = 2,
  };

  uint16_t state;
  uint32_t num_shards;

  uint64_t instance_id{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(state, bl);
    encode(num_shards, bl);
    encode(instance_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(2, bl);
     decode(state, bl);
     decode(num_shards, bl);
     if (struct_v >= 2) {
       decode(instance_id, bl);
     }
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    string s;
    switch ((SyncState)state) {
      case StateInit:
	s = "init";
	break;
      case StateBuildingFullSyncMaps:
	s = "building-full-sync-maps";
	break;
      case StateSync:
	s = "sync";
	break;
      default:
	s = "unknown";
	break;
    }
    encode_json("status", s, f);
    encode_json("num_shards", num_shards, f);
    encode_json("instance_id", instance_id, f);
  }
  void decode_json(JSONObj *obj) {
    std::string s;
    JSONDecoder::decode_json("status", s, obj);
    if (s == "building-full-sync-maps") {
      state = StateBuildingFullSyncMaps;
    } else if (s == "sync") {
      state = StateSync;
    } else {
      state = StateInit;
    }
    JSONDecoder::decode_json("num_shards", num_shards, obj);
    JSONDecoder::decode_json("instance_id", instance_id, obj);
  }
  static void generate_test_instances(std::list<rgw_data_sync_info*>& o);

  rgw_data_sync_info() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(rgw_data_sync_info)

struct rgw_data_sync_marker {
  enum SyncState {
    FullSync = 0,
    IncrementalSync = 1,
  };
  uint16_t state;
  string marker;
  string next_step_marker;
  uint64_t total_entries;
  uint64_t pos;
  real_time timestamp;

  rgw_data_sync_marker() : state(FullSync), total_entries(0), pos(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(state, bl);
    encode(marker, bl);
    encode(next_step_marker, bl);
    encode(total_entries, bl);
    encode(pos, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(state, bl);
    decode(marker, bl);
    decode(next_step_marker, bl);
    decode(total_entries, bl);
    decode(pos, bl);
    decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    const char *s{nullptr};
    switch ((SyncState)state) {
      case FullSync:
        s = "full-sync";
        break;
      case IncrementalSync:
        s = "incremental-sync";
        break;
      default:
        s = "unknown";
        break;
    }
    encode_json("status", s, f);
    encode_json("marker", marker, f);
    encode_json("next_step_marker", next_step_marker, f);
    encode_json("total_entries", total_entries, f);
    encode_json("pos", pos, f);
    encode_json("timestamp", utime_t(timestamp), f);
  }
  void decode_json(JSONObj *obj) {
    std::string s;
    JSONDecoder::decode_json("status", s, obj);
    if (s == "full-sync") {
      state = FullSync;
    } else if (s == "incremental-sync") {
      state = IncrementalSync;
    }
    JSONDecoder::decode_json("marker", marker, obj);
    JSONDecoder::decode_json("next_step_marker", next_step_marker, obj);
    JSONDecoder::decode_json("total_entries", total_entries, obj);
    JSONDecoder::decode_json("pos", pos, obj);
    utime_t t;
    JSONDecoder::decode_json("timestamp", t, obj);
    timestamp = t.to_real_time();
  }
  static void generate_test_instances(std::list<rgw_data_sync_marker*>& o);
};
WRITE_CLASS_ENCODER(rgw_data_sync_marker)

struct rgw_data_sync_status {
  rgw_data_sync_info sync_info;
  map<uint32_t, rgw_data_sync_marker> sync_markers;

  rgw_data_sync_status() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(sync_info, bl);
    /* sync markers are encoded separately */
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(sync_info, bl);
    /* sync markers are decoded separately */
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("info", sync_info, f);
    encode_json("markers", sync_markers, f);
  }
  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("info", sync_info, obj);
    JSONDecoder::decode_json("markers", sync_markers, obj);
  }
  static void generate_test_instances(std::list<rgw_data_sync_status*>& o);
};
WRITE_CLASS_ENCODER(rgw_data_sync_status)

struct rgw_datalog_entry {
  string key;
  ceph::real_time timestamp;

  void decode_json(JSONObj *obj);
};

struct rgw_datalog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_datalog_entry> entries;

  void decode_json(JSONObj *obj);
};

class RGWAsyncRadosProcessor;
class RGWDataSyncControlCR;

struct rgw_bucket_entry_owner {
  string id;
  string display_name;

  rgw_bucket_entry_owner() {}
  rgw_bucket_entry_owner(const string& _id, const string& _display_name) : id(_id), display_name(_display_name) {}

  void decode_json(JSONObj *obj);
};

class RGWSyncErrorLogger;
class RGWRESTConn;
class RGWServices;

struct RGWDataSyncEnv {
  const DoutPrefixProvider *dpp{nullptr};
  CephContext *cct{nullptr};
  rgw::sal::RGWRadosStore *store{nullptr};
  RGWServices *svc{nullptr};
  RGWAsyncRadosProcessor *async_rados{nullptr};
  RGWHTTPManager *http_manager{nullptr};
  RGWSyncErrorLogger *error_logger{nullptr};
  RGWSyncTraceManager *sync_tracer{nullptr};
  RGWSyncModuleInstanceRef sync_module{nullptr};
  PerfCounters* counters{nullptr};

  RGWDataSyncEnv() {}

  void init(const DoutPrefixProvider *_dpp, CephContext *_cct, rgw::sal::RGWRadosStore *_store, RGWServices *_svc,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
            RGWSyncErrorLogger *_error_logger, RGWSyncTraceManager *_sync_tracer,
            RGWSyncModuleInstanceRef& _sync_module,
            PerfCounters* _counters) {
     dpp = _dpp;
    cct = _cct;
    store = _store;
    svc = _svc;
    async_rados = _async_rados;
    http_manager = _http_manager;
    error_logger = _error_logger;
    sync_tracer = _sync_tracer;
    sync_module = _sync_module;
    counters = _counters;
  }

  string shard_obj_name(int shard_id);
  string status_oid();
};

struct RGWDataSyncCtx {
  CephContext *cct{nullptr};
  RGWDataSyncEnv *env{nullptr};

  RGWRESTConn *conn{nullptr};
  string source_zone;

  void init(RGWDataSyncEnv *_env,
            RGWRESTConn *_conn,
            const string& _source_zone) {
    cct = _env->cct;
    env = _env;
    conn = _conn;
    source_zone = _source_zone;
  }
};

class RGWRados;
class RGWDataChangesLogInfo;

class RGWRemoteDataLog : public RGWCoroutinesManager {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  CephContext *cct;
  RGWCoroutinesManagerRegistry *cr_registry;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager http_manager;

  RGWDataSyncEnv sync_env;
  RGWDataSyncCtx sc;

  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWRemoteDataLog::lock");
  RGWDataSyncControlCR *data_sync_cr;

  RGWSyncTraceNodeRef tn;

  bool initialized;

public:
  RGWRemoteDataLog(const DoutPrefixProvider *dpp,
                   rgw::sal::RGWRadosStore *_store,
                   RGWAsyncRadosProcessor *async_rados);
  int init(const string& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger,
           RGWSyncTraceManager *_sync_tracer, RGWSyncModuleInstanceRef& module,
           PerfCounters* _counters);
  void finish();

  int read_log_info(rgw_datalog_info *log_info);
  int read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info);
  int read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result);
  int read_sync_status(rgw_data_sync_status *sync_status);
  int read_recovering_shards(const int num_shards, set<int>& recovering_shards);
  int read_shard_status(int shard_id, set<string>& lagging_buckets,set<string>& recovering_buckets, rgw_data_sync_marker* sync_marker, const int max_entries);
  int init_sync_status(int num_shards);
  int run_sync(int num_shards);

  void wakeup(int shard_id, set<string>& keys);
};

class RGWDataSyncStatusManager : public DoutPrefixProvider {
  rgw::sal::RGWRadosStore *store;

  string source_zone;
  RGWRESTConn *conn;
  RGWSyncErrorLogger *error_logger;
  RGWSyncModuleInstanceRef sync_module;
  PerfCounters* counters;

  RGWRemoteDataLog source_log;

  string source_status_oid;
  string source_shard_status_oid_prefix;

  map<int, rgw_raw_obj> shard_objs;

  int num_shards;

public:
  RGWDataSyncStatusManager(rgw::sal::RGWRadosStore *_store, RGWAsyncRadosProcessor *async_rados,
                           const string& _source_zone, PerfCounters* counters)
    : store(_store), source_zone(_source_zone), conn(NULL), error_logger(NULL),
      sync_module(nullptr), counters(counters),
      source_log(this, store, async_rados), num_shards(0) {}
  RGWDataSyncStatusManager(rgw::sal::RGWRadosStore *_store, RGWAsyncRadosProcessor *async_rados,
                           const string& _source_zone, PerfCounters* counters,
                           const RGWSyncModuleInstanceRef& _sync_module)
    : store(_store), source_zone(_source_zone), conn(NULL), error_logger(NULL),
      sync_module(_sync_module), counters(counters),
      source_log(this, store, async_rados), num_shards(0) {}
  ~RGWDataSyncStatusManager() {
    finalize();
  }
  int init();
  void finalize();

  static string shard_obj_name(const string& source_zone, int shard_id);
  static string sync_status_oid(const string& source_zone);

  int read_sync_status(rgw_data_sync_status *sync_status) {
    return source_log.read_sync_status(sync_status);
  }

  int read_recovering_shards(const int num_shards, set<int>& recovering_shards) {
    return source_log.read_recovering_shards(num_shards, recovering_shards);
  }

  int read_shard_status(int shard_id, set<string>& lagging_buckets, set<string>& recovering_buckets, rgw_data_sync_marker *sync_marker, const int max_entries) {
    return source_log.read_shard_status(shard_id, lagging_buckets, recovering_buckets,sync_marker, max_entries);
  }
  int init_sync_status() { return source_log.init_sync_status(num_shards); }

  int read_log_info(rgw_datalog_info *log_info) {
    return source_log.read_log_info(log_info);
  }
  int read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info) {
    return source_log.read_source_log_shards_info(shards_info);
  }
  int read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result) {
    return source_log.read_source_log_shards_next(shard_markers, result);
  }

  int run() { return source_log.run_sync(num_shards); }

  void wakeup(int shard_id, set<string>& keys) { return source_log.wakeup(shard_id, keys); }
  void stop() {
    source_log.finish();
  }

  // implements DoutPrefixProvider
  CephContext *get_cct() const override;
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;
};

class RGWBucketPipeSyncStatusManager;
class RGWBucketSyncCR;

struct rgw_bucket_shard_full_sync_marker {
  rgw_obj_key position;
  uint64_t count;

  rgw_bucket_shard_full_sync_marker() : count(0) {}

  void encode_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(position, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(position, bl);
    decode(count, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_full_sync_marker)

struct rgw_bucket_shard_inc_sync_marker {
  string position;

  rgw_bucket_shard_inc_sync_marker() {}

  void encode_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(position, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
    decode(position, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool operator<(const rgw_bucket_shard_inc_sync_marker& m) const {
    return (position < m.position);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_inc_sync_marker)

struct rgw_bucket_shard_sync_info {
  enum SyncState {
    StateInit = 0,
    StateFullSync = 1,
    StateIncrementalSync = 2,
  };

  uint16_t state;
  rgw_bucket_shard_full_sync_marker full_marker;
  rgw_bucket_shard_inc_sync_marker inc_marker;

  void decode_from_attrs(CephContext *cct, map<string, bufferlist>& attrs);
  void encode_all_attrs(map<string, bufferlist>& attrs);
  void encode_state_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(state, bl);
    encode(full_marker, bl);
    encode(inc_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(state, bl);
     decode(full_marker, bl);
     decode(inc_marker, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  rgw_bucket_shard_sync_info() : state((int)StateInit) {}

};
WRITE_CLASS_ENCODER(rgw_bucket_shard_sync_info)

struct rgw_bucket_index_marker_info {
  string bucket_ver;
  string master_ver;
  string max_marker;
  bool syncstopped{false};

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket_ver", bucket_ver, obj);
    JSONDecoder::decode_json("master_ver", master_ver, obj);
    JSONDecoder::decode_json("max_marker", max_marker, obj);
    JSONDecoder::decode_json("syncstopped", syncstopped, obj);
  }
};


class RGWRemoteBucketManager {
  const DoutPrefixProvider *dpp;

  RGWDataSyncEnv *sync_env;

  RGWRESTConn *conn{nullptr};
  string source_zone;

  vector<rgw_bucket_sync_pair_info> sync_pairs;

  RGWDataSyncCtx sc;
  rgw_bucket_shard_sync_info init_status;

  RGWBucketSyncCR *sync_cr{nullptr};

public:
  RGWRemoteBucketManager(const DoutPrefixProvider *_dpp,
                     RGWDataSyncEnv *_sync_env,
                     const string& _source_zone, RGWRESTConn *_conn,
                     const RGWBucketInfo& source_bucket_info,
                     const rgw_bucket& dest_bucket);

  void init(const string& _source_zone, RGWRESTConn *_conn,
            const rgw_bucket& source_bucket, int shard_id,
            const rgw_bucket& dest_bucket);

  RGWCoroutine *read_sync_status_cr(int num, rgw_bucket_shard_sync_info *sync_status);
  RGWCoroutine *init_sync_status_cr(int num);
  RGWCoroutine *run_sync_cr(int num);

  int num_pipes() {
    return sync_pairs.size();
  }

  void wakeup();
};

class RGWBucketPipeSyncStatusManager : public DoutPrefixProvider {
  rgw::sal::RGWRadosStore *store;

  RGWDataSyncEnv sync_env;

  RGWCoroutinesManager cr_mgr;

  RGWHTTPManager http_manager;

  std::optional<string> source_zone;
  std::optional<rgw_bucket> source_bucket;

  RGWRESTConn *conn;
  RGWSyncErrorLogger *error_logger;
  RGWSyncModuleInstanceRef sync_module;

  rgw_bucket dest_bucket;

  vector<RGWRemoteBucketManager *> source_mgrs;

  string source_status_oid;
  string source_shard_status_oid_prefix;

  map<int, rgw_bucket_shard_sync_info> sync_status;
  rgw_raw_obj status_obj;

  int num_shards;

public:
  RGWBucketPipeSyncStatusManager(rgw::sal::RGWRadosStore *_store,
                             std::optional<string> _source_zone,
                             std::optional<rgw_bucket> _source_bucket,
                             const rgw_bucket& dest_bucket);
  ~RGWBucketPipeSyncStatusManager();

  int init();

  map<int, rgw_bucket_shard_sync_info>& get_sync_status() { return sync_status; }
  int init_sync_status();

  static string status_oid(const string& source_zone, const rgw_bucket_sync_pair_info& bs);
  static string obj_status_oid(const string& source_zone, const rgw_obj& obj); /* can be used by sync modules */

  // implements DoutPrefixProvider
  CephContext *get_cct() const override;
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;

  int read_sync_status();
  int run();
};

/// read the sync status of all bucket shards from the given source zone
int rgw_bucket_sync_status(const DoutPrefixProvider *dpp,
                           rgw::sal::RGWRadosStore *store,
                           const rgw_sync_bucket_pipe& pipe,
                           const RGWBucketInfo& dest_bucket_info,
                           std::vector<rgw_bucket_shard_sync_info> *status);

class RGWDefaultSyncModule : public RGWSyncModule {
public:
  RGWDefaultSyncModule() {}
  bool supports_writes() override { return true; }
  bool supports_data_export() override { return true; }
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

class RGWArchiveSyncModule : public RGWDefaultSyncModule {
public:
  RGWArchiveSyncModule() {}
  bool supports_writes() override { return true; }
  bool supports_data_export() override { return false; }
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif
