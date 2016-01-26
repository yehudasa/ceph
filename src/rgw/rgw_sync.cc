#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/admin_socket.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_sync.h"
#include "rgw_metadata.h"
#include "rgw_rest_conn.h"
#include "rgw_tools.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_http_client.h"
#include "rgw_boost_asio_yield.h"

#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rgw

static string mdlog_sync_status_oid = "mdlog.sync-status";
static string mdlog_sync_status_shard_prefix = "mdlog.sync-status.shard";
static string mdlog_sync_full_sync_index_prefix = "meta.full-sync.index";

struct rgw_sync_error_info {
  uint32_t error_code;
  string message;

  rgw_sync_error_info() : error_code(0) {}
  rgw_sync_error_info(uint32_t _error_code, const string& _message) : error_code(_error_code), message(_message) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(error_code, bl);
    ::encode(message, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(error_code, bl);
    ::decode(message, bl);
    DECODE_FINISH(bl);
  } 
};
WRITE_CLASS_ENCODER(rgw_sync_error_info)

RGWSyncErrorLogger::RGWSyncErrorLogger(RGWRados *_store, const string oid_prefix, int _num_shards) : store(_store), num_shards(_num_shards) {
  oids.reserve(num_shards);

  char buf[oid_prefix.size() + 16];

  for (int i = 0; i < num_shards; i++) {
    snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), i);
  }
}

RGWCoroutine *RGWSyncErrorLogger::log_error_cr(const string& section, const string& name, uint32_t error_code, const string& message) {
  cls_log_entry entry;

  rgw_sync_error_info info(error_code, message);
  bufferlist bl;
  ::encode(info, bl);
  store->time_log_prepare_entry(entry, ceph_clock_now(store->ctx()), section, name, bl);

  uint32_t shard_id = counter.inc() % num_shards;


  return new RGWRadosTimelogAddCR(store, oids[shard_id], entry);
}

void RGWSyncBackoff::update_wait_time()
{
  if (cur_wait == 0) {
    cur_wait = 1;
  } else {
    cur_wait = (cur_wait << 1);
  }
  if (cur_wait >= max_secs) {
    cur_wait = max_secs;
  }
}

void RGWSyncBackoff::backoff_sleep()
{
  update_wait_time();
  sleep(cur_wait);
}

void RGWSyncBackoff::backoff(RGWCoroutine *op)
{
  update_wait_time();
  op->wait(utime_t(cur_wait, 0));
}

int RGWBackoffControlCR::operate() {
  RGWCoroutine *finisher_cr;
  reenter(this) {
    while (true) {
      yield {
        Mutex::Locker l(lock);
        cr = alloc_cr();
        cr->get();
        call(cr);
      }
      {
        Mutex::Locker l(lock);
        cr->put();
        cr = NULL;
      }
      if (retcode < 0 && retcode != -EBUSY && retcode != -EAGAIN) {
        ldout(cct, 0) << "ERROR: RGWBackoffControlCR called coroutine returned " << retcode << dendl;
        return set_cr_error(retcode);
      }
      if (reset_backoff) {
        backoff.reset();
      }
      yield backoff.backoff(this);
      finisher_cr = alloc_finisher_cr();
      if (finisher_cr) {
        yield call(finisher_cr);
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: call to finisher_cr() failed: retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }
      }
    }
  }
  return 0;
}

void rgw_mdlog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
}

struct rgw_mdlog_entry {
  string id;
  string section;
  string name;
  utime_t timestamp;
  RGWMetadataLogData log_data;

  void decode_json(JSONObj *obj);

  bool convert_from(cls_log_entry& le) {
    id = le.id;
    section = le.section;
    name = le.name;
    timestamp = le.timestamp;
    try {
      bufferlist::iterator iter = le.data.begin();
      ::decode(log_data, iter);
    } catch (buffer::error& err) {
      return false;
    }
    return true;
  }
};

struct rgw_mdlog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_mdlog_entry> entries;

  void decode_json(JSONObj *obj);
};


void rgw_mdlog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("section", section, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
  JSONDecoder::decode_json("data", log_data, obj);
}

void rgw_mdlog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

RGWRemoteMetaLog::~RGWRemoteMetaLog()
{
  delete error_logger;
}

int RGWRemoteMetaLog::read_log_info(rgw_mdlog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { NULL, NULL } };

  int ret = conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteMetaLog::init()
{
  conn = store->rest_master_conn;

  int ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  init_sync_env(&sync_env);

  return 0;
}

void RGWRemoteMetaLog::finish()
{
  going_down.set(1);
  stop();
}

int RGWRemoteMetaLog::list_shards(int num_shards)
{
  for (int i = 0; i < (int)num_shards; i++) {
    int ret = list_shard(i);
    if (ret < 0) {
      ldout(store->ctx(), 10) << "failed to list shard: ret=" << ret << dendl;
    }
  }

  return 0;
}

int RGWRemoteMetaLog::list_shard(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { NULL, NULL } };

  rgw_mdlog_shard_data data;
  int ret = conn->get_json_resource("/admin/log", pairs, data);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog data" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  vector<rgw_mdlog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_mdlog_entry& entry = *iter;
    ldout(store->ctx(), 20) << "entry: name=" << entry.name << dendl;
  }

  return 0;
}

int RGWRemoteMetaLog::get_shard_info(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { "info", NULL },
                                  { NULL, NULL } };

  RGWMetadataLogInfo info;
  int ret = conn->get_json_resource("/admin/log", pairs, info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " marker=" << info.marker << dendl;

  return 0;
}

#define CLONE_MAX_ENTRIES 100

int RGWMetaSyncStatusManager::init()
{
  if (store->is_meta_master()) {
    return 0;
  }

  if (!store->rest_master_conn) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << dendl;
    return r;
  }

  r = master_log.init();
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to init remote log, r=" << r << dendl;
    return r;
  }

  RGWMetaSyncEnv& sync_env = master_log.get_sync_env();

  r = read_sync_status();
  if (r < 0 && r != -ENOENT) {
    lderr(store->ctx()) << "ERROR: failed to read sync status, r=" << r << dendl;
    return r;
  }

  num_shards = sync_status.sync_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_obj(store->get_zone_params().log_pool, sync_env.shard_obj_name(i));
  }

  RWLock::WLocker wl(ts_to_shard_lock);
  for (int i = 0; i < num_shards; i++) {
    clone_markers.push_back(string());
    utime_shard ut;
    ut.shard_id = i;
    ts_to_shard[ut] = i;
  }

  return 0;
}

void RGWMetaSyncEnv::init(CephContext *_cct, RGWRados *_store, RGWRESTConn *_conn,
                          RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
                          RGWSyncErrorLogger *_error_logger) {
  cct = _cct;
  store = _store;
  conn = _conn;
  async_rados = _async_rados;
  http_manager = _http_manager;
  error_logger = _error_logger;
}

string RGWMetaSyncEnv::status_oid()
{
  return mdlog_sync_status_oid;
}

string RGWMetaSyncEnv::shard_obj_name(int shard_id)
{
  char buf[mdlog_sync_status_shard_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_status_shard_prefix.c_str(), shard_id);

  return string(buf);
}

class RGWReadMDLogShardInfo : public RGWSimpleCoroutine {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  int req_ret;

  int shard_id;
  RGWMetadataLogInfo *shard_info;
public:
  RGWReadMDLogShardInfo(RGWRados *_store, int _shard_id, RGWMetadataLogInfo *_shard_info) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store), mdlog(store->meta_mgr->get_log()),
                                                req_ret(0), shard_id(_shard_id), shard_info(_shard_info) {
  }

  int send_request() {
    int ret = mdlog->get_info_async(shard_id, shard_info, stack->get_completion_mgr(), (void *)stack, &req_ret);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
      return set_cr_error(ret);
    }

    return 0;
  }

  int request_complete() {
    return req_ret;
  }
};

class RGWAsyncReadMDLogEntries : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  int shard_id;
  string *marker;
  int max_entries;
  list<cls_log_entry> *entries;
  bool *truncated;

protected:
  int _send_request() {
    utime_t from_time;
    utime_t end_time;

    void *handle;

    mdlog->init_list_entries(shard_id, from_time, end_time, *marker, &handle);

    int ret = mdlog->list_entries(handle, max_entries, *entries, marker, truncated);

    mdlog->complete_list_entries(handle);

    return ret;
  }
public:
  RGWAsyncReadMDLogEntries(RGWAioCompletionNotifier *cn, RGWRados *_store,
			   int _shard_id, string* _marker, int _max_entries,
			   list<cls_log_entry> *_entries, bool *_truncated) : RGWAsyncRadosRequest(cn), store(_store), mdlog(store->meta_mgr->get_log()),
                                                                   shard_id(_shard_id), marker(_marker), max_entries(_max_entries),
								   entries(_entries), truncated(_truncated) {}
};

class RGWReadMDLogEntriesCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  int shard_id;
  string marker;
  string *pmarker;
  int max_entries;
  list<cls_log_entry> *entries;
  bool *truncated;

  RGWAsyncReadMDLogEntries *req;

public:
  RGWReadMDLogEntriesCR(RGWMetaSyncEnv *_sync_env,
		       int _shard_id, string*_marker, int _max_entries,
		       list<cls_log_entry> *_entries, bool *_truncated) : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                shard_id(_shard_id), pmarker(_marker), max_entries(_max_entries),
						entries(_entries), truncated(_truncated) {
						}

  ~RGWReadMDLogEntriesCR() {
    if (req) {
      req->finish();
    }
  }

  int send_request() {
    marker = *pmarker;
    req = new RGWAsyncReadMDLogEntries(stack->create_completion_notifier(),
			           sync_env->store, shard_id, &marker, max_entries, entries, truncated);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    int ret = req->get_ret_status();
    if (ret >= 0 && !entries->empty()) {
     *pmarker = marker;
    }
    return req->get_ret_status();
  }
};


class RGWReadRemoteMDLogShardInfoCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTReadResource *http_op;

  int shard_id;
  RGWMetadataLogInfo *shard_info;

public:
  RGWReadRemoteMDLogShardInfoCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                                      int _shard_id, RGWMetadataLogInfo *_shard_info) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  int operate() {
    RGWRESTConn *conn = store->rest_master_conn;
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "metadata" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info);
        if (ret < 0) {
          return set_cr_error(ret);
        }
        return set_cr_done();
      }
    }
    return 0;
  }
};

class RGWInitSyncStatusCoroutine : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;
  RGWObjectCtx& obj_ctx;

  rgw_meta_sync_info status;
  map<int, RGWMetadataLogInfo> shards_info;
  RGWContinuousLeaseCR *lease_cr;
public:
  RGWInitSyncStatusCoroutine(RGWMetaSyncEnv *_sync_env,
		      RGWObjectCtx& _obj_ctx, uint32_t _num_shards) : RGWCoroutine(_sync_env->store->ctx()), sync_env(_sync_env),
                                                obj_ctx(_obj_ctx), lease_cr(NULL) {
    status.num_shards = _num_shards;
  }

  ~RGWInitSyncStatusCoroutine() {
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
        set_status("acquiring sync lock");
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
        RGWRados *store = sync_env->store;
	lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_env->status_oid(),
                                            lock_name, lock_duration, this);
        lease_cr->get();
        spawn(lease_cr, false);
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
          set_status("lease lock failed, early abort");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      yield {
        set_status("writing sync status");
        RGWRados *store = sync_env->store;
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 sync_env->status_oid(), status));
      }

      if (retcode < 0) {
        set_status("failed to write sync status");
        ldout(cct, 0) << "ERROR: failed to write sync status, retcode=" << retcode << dendl;
        yield lease_cr->go_down();
        return set_cr_error(retcode);
      }
      /* fetch current position in logs */
      set_status("fetching remote log position");
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
          spawn(new RGWReadRemoteMDLogShardInfoCR(sync_env->store, sync_env->http_manager, sync_env->async_rados, i, &shards_info[i]), false);
	}
      }

      drain_all_but(1); /* the lease cr still needs to run */

      yield {
        set_status("updating sync status");
        for (int i = 0; i < (int)status.num_shards; i++) {
	  rgw_meta_sync_marker marker;
          RGWMetadataLogInfo& info = shards_info[i];
	  marker.next_step_marker = info.marker;
	  marker.timestamp = info.last_update;
          RGWRados *store = sync_env->store;
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				                          sync_env->shard_obj_name(i), marker), true);
        }
      }
      yield {
        set_status("changing sync state: build full sync maps");
	status.state = rgw_meta_sync_info::StateBuildingFullSyncMaps;
        RGWRados *store = sync_env->store;
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 sync_env->status_oid(), status));
      }
      set_status("drop lock lease");
      yield lease_cr->go_down();
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_cr_error(ret);
	}
        yield;
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWReadSyncStatusCoroutine : public RGWSimpleRadosReadCR<rgw_meta_sync_info> {
  RGWMetaSyncEnv *sync_env;
  RGWObjectCtx& obj_ctx;

  rgw_meta_sync_status *sync_status;

public:
  RGWReadSyncStatusCoroutine(RGWMetaSyncEnv *_sync_env,
		      RGWObjectCtx& _obj_ctx,
		      rgw_meta_sync_status *_status) : RGWSimpleRadosReadCR(_sync_env->async_rados, _sync_env->store, _obj_ctx,
									    _sync_env->store->get_zone_params().log_pool,
									    _sync_env->status_oid(),
									    &_status->sync_info),
                                                                            sync_env(_sync_env),
                                                                            obj_ctx(_obj_ctx),
									    sync_status(_status) {

  }

  int handle_data(rgw_meta_sync_info& data);
};

int RGWReadSyncStatusCoroutine::handle_data(rgw_meta_sync_info& data)
{
  if (retcode == -ENOENT) {
    return retcode;
  }

  RGWRados *store = sync_env->store;
  map<uint32_t, rgw_meta_sync_marker>& markers = sync_status->sync_markers;
  for (int i = 0; i < (int)data.num_shards; i++) {
    spawn(new RGWSimpleRadosReadCR<rgw_meta_sync_marker>(sync_env->async_rados, store, obj_ctx, store->get_zone_params().log_pool,
				                    sync_env->shard_obj_name(i), &markers[i]), true);
  }
  return 0;
}

class RGWFetchAllMetaCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  int num_shards;


  int ret_status;

  list<string> sections;
  list<string>::iterator sections_iter;
  list<string> result;
  list<string>::iterator iter;

  RGWShardedOmapCRManager *entries_index;

  RGWContinuousLeaseCR *lease_cr;
  bool lost_lock;
  bool failed;

  map<uint32_t, rgw_meta_sync_marker>& markers;

public:
  RGWFetchAllMetaCR(RGWMetaSyncEnv *_sync_env, int _num_shards,
                    map<uint32_t, rgw_meta_sync_marker>& _markers) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
						      num_shards(_num_shards),
						      ret_status(0), entries_index(NULL), lease_cr(NULL), lost_lock(false), failed(false), markers(_markers) {
  }

  ~RGWFetchAllMetaCR() {
    if (lease_cr) {
      lease_cr->put();
    }
  }

  void append_section_from_set(set<string>& all_sections, const string& name) {
    set<string>::iterator iter = all_sections.find(name);
    if (iter != all_sections.end()) {
      sections.push_back(name);
      all_sections.erase(iter);
    }
  }
  /*
   * meta sync should go in the following order: user, bucket.instance, bucket
   * then whatever other sections exist (if any)
   */
  void rearrange_sections() {
    set<string> all_sections;
    for (list<string>::iterator iter = sections.begin(); iter != sections.end(); ++iter) {
      all_sections.insert(*iter);
    }
    sections.clear();
    append_section_from_set(all_sections, "user");
    append_section_from_set(all_sections, "bucket.instance");
    append_section_from_set(all_sections, "bucket");

    for (set<string>::iterator iter = all_sections.begin(); iter != all_sections.end(); ++iter) {
      sections.push_back(*iter);
    }
  }

  int operate() {
    RGWRESTConn *conn = sync_env->conn;

    reenter(this) {
      yield {
        set_status(string("acquiring lock (") + sync_env->status_oid() + ")");
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
	lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, sync_env->store, sync_env->store->get_zone_params().log_pool, sync_env->status_oid(),
                                            lock_name, lock_duration, this);
        lease_cr->get();
        spawn(lease_cr, false);
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
          set_status("failed acquiring lock");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      entries_index = new RGWShardedOmapCRManager(sync_env->async_rados, sync_env->store, this, num_shards,
						  sync_env->store->get_zone_params().log_pool,
                                                  mdlog_sync_full_sync_index_prefix);
      yield {
	call(new RGWReadRESTResourceCR<list<string> >(cct, conn, sync_env->http_manager,
				       "/admin/metadata", NULL, &sections));
      }
      if (get_ret_status() < 0) {
        ldout(cct, 0) << "ERROR: failed to fetch metadata sections" << dendl;
        yield lease_cr->go_down();
        drain_all();
	return set_cr_error(get_ret_status());
      }
      rearrange_sections();
      sections_iter = sections.begin();
      for (; sections_iter != sections.end(); ++sections_iter) {
        yield {
	  string entrypoint = string("/admin/metadata/") + *sections_iter;
#warning need a better scaling solution here, requires streaming output
	  call(new RGWReadRESTResourceCR<list<string> >(cct, conn, sync_env->http_manager,
				       entrypoint, NULL, &result));
	}
        if (get_ret_status() < 0) {
          ldout(cct, 0) << "ERROR: failed to fetch metadata section: " << *sections_iter << dendl;
          yield lease_cr->go_down();
          drain_all();
          return set_cr_error(get_ret_status());
        }
        iter = result.begin();
        for (; iter != result.end(); ++iter) {
          RGWRados *store;
          int ret;
          yield {
            if (!lease_cr->is_locked()) {
              lost_lock = true;
              break;
            }
	    ldout(cct, 20) << "list metadata: section=" << *sections_iter << " key=" << *iter << dendl;
	    string s = *sections_iter + ":" + *iter;
            int shard_id;
            store = sync_env->store;
            ret = store->meta_mgr->get_log_shard_id(*sections_iter, *iter, &shard_id);
            if (ret < 0) {
              ldout(cct, 0) << "ERROR: could not determine shard id for " << *sections_iter << ":" << *iter << dendl;
              ret_status = ret;
              break;
            }
	    if (!entries_index->append(s, shard_id)) {
              break;
            }
	  }
	}
      }
      yield {
        if (!entries_index->finish()) {
          failed = true;
        }
      }
      if (!failed) {
        for (map<uint32_t, rgw_meta_sync_marker>::iterator iter = markers.begin(); iter != markers.end(); ++iter) {
          int shard_id = (int)iter->first;
          rgw_meta_sync_marker& marker = iter->second;
          marker.total_entries = entries_index->get_total_entries(shard_id);
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados, sync_env->store, sync_env->store->get_zone_params().log_pool,
                                                                sync_env->shard_obj_name(shard_id), marker), true);
        }
      }

      drain_all_but(1); /* the lease cr still needs to run */

      yield lease_cr->go_down();

      int ret;
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_cr_error(ret);
	}
        yield;
      }
      if (failed) {
        yield return set_cr_error(-EIO);
      }
      if (lost_lock) {
        yield return set_cr_error(-EBUSY);
      }

      if (ret_status < 0) {
        yield return set_cr_error(ret_status);
      }

      yield return set_cr_done();
    }
    return 0;
  }
};

static string full_sync_index_shard_oid(int shard_id)
{
  char buf[mdlog_sync_full_sync_index_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_full_sync_index_prefix.c_str(), shard_id);
  return string(buf);
}

class RGWReadRemoteMetadataCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  RGWRESTReadResource *http_op;

  string section;
  string key;

  bufferlist *pbl;

public:
  RGWReadRemoteMetadataCR(RGWMetaSyncEnv *_sync_env,
                                                      const string& _section, const string& _key, bufferlist *_pbl) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                      http_op(NULL),
                                                      section(_section),
                                                      key(_key),
						      pbl(_pbl) {
  }

  int operate() {
    RGWRESTConn *conn = sync_env->conn;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "key" , key.c_str()},
	                                { NULL, NULL } };

        string p = string("/admin/metadata/") + section + "/" + key;

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, sync_env->http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to fetch mdlog data" << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait_bl(pbl);
        if (ret < 0) {
          return set_cr_error(ret);
        }
        return set_cr_done();
      }
    }
    return 0;
  }
};

class RGWAsyncMetaStoreEntry : public RGWAsyncRadosRequest {
  RGWRados *store;
  string raw_key;
  bufferlist bl;
protected:
  int _send_request() {
    int ret = store->meta_mgr->put(raw_key, bl, RGWMetadataHandler::APPLY_ALWAYS);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: can't store key: " << raw_key << " ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
public:
  RGWAsyncMetaStoreEntry(RGWAioCompletionNotifier *cn, RGWRados *_store,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWAsyncRadosRequest(cn), store(_store),
                                          raw_key(_raw_key), bl(_bl) {}
};


class RGWMetaStoreEntryCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  string raw_key;
  bufferlist bl;

  RGWAsyncMetaStoreEntry *req;

public:
  RGWMetaStoreEntryCR(RGWMetaSyncEnv *_sync_env,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
                                          raw_key(_raw_key), bl(_bl), req(NULL) {
  }

  ~RGWMetaStoreEntryCR() {
    if (req) {
      req->finish();
    }
  }

  int send_request() {
    req = new RGWAsyncMetaStoreEntry(stack->create_completion_notifier(),
			           sync_env->store, raw_key, bl);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWAsyncMetaRemoveEntry : public RGWAsyncRadosRequest {
  RGWRados *store;
  string raw_key;
protected:
  int _send_request() {
    int ret = store->meta_mgr->remove(raw_key);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: can't remove key: " << raw_key << " ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
public:
  RGWAsyncMetaRemoveEntry(RGWAioCompletionNotifier *cn, RGWRados *_store,
                       const string& _raw_key) : RGWAsyncRadosRequest(cn), store(_store),
                                          raw_key(_raw_key) {}
};


class RGWMetaRemoveEntryCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  string raw_key;

  RGWAsyncMetaRemoveEntry *req;

public:
  RGWMetaRemoveEntryCR(RGWMetaSyncEnv *_sync_env,
                       const string& _raw_key) : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
                                          raw_key(_raw_key), req(NULL) {
  }

  ~RGWMetaRemoveEntryCR() {
    if (req) {
      req->finish();
    }
  }

  int send_request() {
    req = new RGWAsyncMetaRemoveEntry(stack->create_completion_notifier(),
			           sync_env->store, raw_key);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    int r = req->get_ret_status();
    if (r == -ENOENT) {
      r = 0;
    }
    return r;
  }
};

#define META_SYNC_UPDATE_MARKER_WINDOW 10

class RGWMetaSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, string> {
  RGWMetaSyncEnv *sync_env;

  string marker_oid;
  rgw_meta_sync_marker sync_marker;


public:
  RGWMetaSyncShardMarkerTrack(RGWMetaSyncEnv *_sync_env,
                         const string& _marker_oid,
                         const rgw_meta_sync_marker& _marker) : RGWSyncShardMarkerTrack(META_SYNC_UPDATE_MARKER_WINDOW),
                                                                sync_env(_sync_env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const utime_t& timestamp) {
    sync_marker.marker = new_marker;
    if (index_pos > 0) {
      sync_marker.pos = index_pos;
    }

    if (timestamp.sec() > 0) {
      sync_marker.timestamp = timestamp;
    }

    ldout(sync_env->cct, 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    RGWRados *store = sync_env->store;
    return new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, sync_marker);
  }
};

int RGWMetaSyncSingleEntryCR::operate() {
  reenter(this) {
#define NUM_TRANSIENT_ERROR_RETRIES 10

    if (op_status != MDLOG_STATUS_COMPLETE) {
      ldout(sync_env->cct, 20) << "skipping pending operation" << dendl;
      yield call(marker_tracker->finish(entry_marker));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    for (tries = 0; tries < NUM_TRANSIENT_ERROR_RETRIES; tries++) {
      yield {
        pos = raw_key.find(':');
        section = raw_key.substr(0, pos);
        key = raw_key.substr(pos + 1);
        ldout(sync_env->cct, 20) << "fetching remote metadata: " << section << ":" << key << (tries == 0 ? "" : " (retry)") << dendl;
        call(new RGWReadRemoteMetadataCR(sync_env, section, key, &md_bl));
      }

      sync_status = retcode;

      if (sync_status == -ENOENT) {
#warning remove entry from local
        break;
      }

      if ((sync_status == -EAGAIN || sync_status == -ECANCELED) && (tries < NUM_TRANSIENT_ERROR_RETRIES - 1)) {
        ldout(sync_env->cct, 20) << *this << ": failed to fetch remote metadata: " << section << ":" << key << ", will retry" << dendl;
        continue;
      }

      if (sync_status < 0) {
#warning need to store entry for non-transient errors
        ldout(sync_env->cct, 10) << *this << ": failed to send read remote metadata entry: section=" << section << " key=" << key << " status=" << sync_status << dendl;
        log_error() << "failed to send read remote metadata entry: section=" << section << " key=" << key << " status=" << sync_status << std::endl;
        return set_cr_error(sync_status);
      }

      break;
    }

    retcode = 0;
    for (tries = 0; tries < NUM_TRANSIENT_ERROR_RETRIES; tries++) {
      if (sync_status != -ENOENT) {
          yield call(new RGWMetaStoreEntryCR(sync_env, raw_key, md_bl));
      } else {
          yield call(new RGWMetaRemoveEntryCR(sync_env, raw_key));
      }
      if ((retcode == -EAGAIN || retcode == -ECANCELED) && (tries < NUM_TRANSIENT_ERROR_RETRIES - 1)) {
        ldout(sync_env->cct, 20) << *this << ": failed to store metadata: " << section << ":" << key << ", got retcode=" << retcode << dendl;
        continue;
      }
      break;
    }

    sync_status = retcode;

    if (sync_status == 0 && marker_tracker) {
      /* update marker */
      yield call(marker_tracker->finish(entry_marker));
      sync_status = retcode;
    }
    if (sync_status < 0) {
      return set_cr_error(sync_status);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWCloneMetaLogCoroutine : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;
  RGWMetadataLog *mdlog;

  int shard_id;
  string marker;
  bool truncated;
  string *new_marker;

  int max_entries;

  RGWRESTReadResource *http_op;

  int req_ret;
  RGWMetadataLogInfo shard_info;
  rgw_mdlog_shard_data data;

public:
  RGWCloneMetaLogCoroutine(RGWMetaSyncEnv *_sync_env,
		    int _id, const string& _marker, string *_new_marker) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                      mdlog(sync_env->store->meta_mgr->get_log()),
                                                      shard_id(_id),
                                                      marker(_marker), truncated(false), new_marker(_new_marker),
                                                      max_entries(CLONE_MAX_ENTRIES),
						      http_op(NULL),
						      req_ret(0) {
    if (new_marker) {
      *new_marker = marker;
    }
  }
  ~RGWCloneMetaLogCoroutine() {
    if (http_op) {
      http_op->put();
    }
  }

  int operate();

  int state_init();
  int state_read_shard_status();
  int state_read_shard_status_complete();
  int state_send_rest_request();
  int state_receive_rest_response();
  int state_store_mdlog_entries();
  int state_store_mdlog_entries_complete();
};

class RGWMetaSyncShardCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  rgw_bucket pool;

  uint32_t shard_id;
  rgw_meta_sync_marker sync_marker;
  string marker;
  string max_marker;

  map<string, bufferlist> entries;
  map<string, bufferlist>::iterator iter;

  string oid;

  RGWMetaSyncShardMarkerTrack *marker_tracker;

  list<cls_log_entry> log_entries;
  list<cls_log_entry>::iterator log_iter;
  bool truncated;

  string mdlog_marker;
  string raw_key;
  rgw_mdlog_entry mdlog_entry;

  Mutex inc_lock;
  Cond inc_cond;

  boost::asio::coroutine incremental_cr;
  boost::asio::coroutine full_cr;

  RGWContinuousLeaseCR *lease_cr;
  bool lost_lock;

  bool *reset_backoff;

  map<RGWCoroutinesStack *, string> stack_to_pos;
  map<string, string> pos_to_prev;

  bool can_adjust_marker;

  int total_entries;

public:
  RGWMetaSyncShardCR(RGWMetaSyncEnv *_sync_env,
		     rgw_bucket& _pool,
		     uint32_t _shard_id, rgw_meta_sync_marker& _marker,
                     bool *_reset_backoff) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker),
                                                      marker_tracker(NULL), truncated(false), inc_lock("RGWMetaSyncShardCR::inc_lock"),
                                                      lease_cr(NULL), lost_lock(false), reset_backoff(_reset_backoff), can_adjust_marker(false),
                                                      total_entries(0) {
    *reset_backoff = false;
  }

  ~RGWMetaSyncShardCR() {
    delete marker_tracker;
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
  }

  void set_marker_tracker(RGWMetaSyncShardMarkerTrack *mt) {
    delete marker_tracker;
    marker_tracker = mt;
  }

  int operate() {
    int r;
    while (true) {
      switch (sync_marker.state) {
      case rgw_meta_sync_marker::FullSync:
        r  = full_sync();
        if (r < 0) {
          ldout(sync_env->cct, 10) << "sync: full_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      case rgw_meta_sync_marker::IncrementalSync:
        r  = incremental_sync();
        if (r < 0) {
          ldout(sync_env->cct, 10) << "sync: incremental_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      }
    }
    /* unreachable */
    return 0;
  }

  void collect_children()
  {
    int child_ret;
    RGWCoroutinesStack *child;
    while (collect_next(&child_ret, &child)) {
      map<RGWCoroutinesStack *, string>::iterator iter = stack_to_pos.find(child);
      if (iter == stack_to_pos.end()) {
        /* some other stack that we don't care about */
        continue;
      }

      string& pos = iter->second;

      if (child_ret < 0) {
        ldout(sync_env->cct, 0) << *this << ": child operation stack=" << child << " entry=" << pos << " returned " << child_ret << dendl;
      }

      map<string, string>::iterator prev_iter = pos_to_prev.find(pos);
      assert(prev_iter != pos_to_prev.end());

      /*
       * we should get -EAGAIN for transient errors, for which we want to retry, so we don't
       * update the marker and abort. We'll get called again for these. Permanent errors will be
       * handled by marking the entry at the error log shard, so that we retry on it separately
       */
      if (child_ret == -EAGAIN) {
        can_adjust_marker = false;
      }

      if (pos_to_prev.size() == 1) {
        if (can_adjust_marker) {
          sync_marker.marker = pos;
        }
        pos_to_prev.erase(prev_iter);
      } else {
        assert(pos_to_prev.size() > 1);
        pos_to_prev.erase(prev_iter);
        prev_iter = pos_to_prev.begin();
        if (can_adjust_marker) {
          sync_marker.marker = prev_iter->second;
        }
      }

      ldout(sync_env->cct, 0) << *this << ": adjusting marker pos=" << sync_marker.marker << dendl;
      stack_to_pos.erase(iter);

      child->put();
    }
  }

  int full_sync() {
#define OMAP_GET_MAX_ENTRIES 100
    int max_entries = OMAP_GET_MAX_ENTRIES;
    reenter(&full_cr) {
      set_status("full_sync");
      oid = full_sync_index_shard_oid(shard_id);
      can_adjust_marker = true;
      /* grab lock */
      yield {
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
        if (lease_cr) {
          lease_cr->put();
        }
        RGWRados *store = sync_env->store;
	lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                            sync_env->shard_obj_name(shard_id),
                                            lock_name, lock_duration, this);
        lease_cr->get();
        spawn(lease_cr, false);
        lost_lock = false;
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
          drain_all();
          return lease_cr->get_ret_status();
        }
        set_sleeping(true);
        yield;
      }

      /* lock succeeded, a retry now should avoid previous backoff status */
      *reset_backoff = true;

      /* prepare marker tracker */
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(sync_env,
                                                         sync_env->shard_obj_name(shard_id),
                                                         sync_marker));

      marker = sync_marker.marker;

      total_entries = sync_marker.pos;

      /* sync! */
      do {
        if (!lease_cr->is_locked()) {
          lost_lock = true;
          break;
        }
        yield call(new RGWRadosGetOmapKeysCR(sync_env->store, pool, oid, marker, &entries, max_entries));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): RGWRadosGetOmapKeysCR() returned ret=" << retcode << dendl;
          yield lease_cr->go_down();
          drain_all();
          return retcode;
        }
        iter = entries.begin();
        for (; iter != entries.end(); ++iter) {
          ldout(sync_env->cct, 20) << __func__ << ": full sync: " << iter->first << dendl;
          total_entries++;
          if (!marker_tracker->start(iter->first, total_entries, utime_t())) {
            ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << iter->first << ". Duplicate entry?" << dendl;
          } else {
            // fetch remote and write locally
            yield {
              RGWCoroutinesStack *stack = spawn(new RGWMetaSyncSingleEntryCR(sync_env, iter->first, iter->first, MDLOG_STATUS_COMPLETE, marker_tracker), false);
              stack->get();

              stack_to_pos[stack] = iter->first;
              pos_to_prev[iter->first] = marker;
            }
          }
          marker = iter->first;
        }
        collect_children();
      } while ((int)entries.size() == max_entries && can_adjust_marker);

      while (num_spawned() > 1) {
        yield wait_for_child();
        collect_children();
      }

      if (!lost_lock) {
        yield {
          /* update marker to reflect we're done with full sync */
          if (can_adjust_marker) {
            sync_marker.state = rgw_meta_sync_marker::IncrementalSync;
            sync_marker.marker = sync_marker.next_step_marker;
            sync_marker.next_step_marker.clear();
          }
          RGWRados *store = sync_env->store;
          ldout(sync_env->cct, 0) << *this << ": saving marker pos=" << sync_marker.marker << dendl;
          call(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                                               sync_env->shard_obj_name(shard_id), sync_marker));
        }
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to set sync marker: retcode=" << retcode << dendl;
          return retcode;
        }
      }

      /* 
       * if we reached here, it means that lost_lock is true, otherwise the state
       * change in the previous block will prevent us from reaching here
       */

      yield lease_cr->go_down();

      lease_cr->put();
      lease_cr = NULL;

      drain_all();

      if (!can_adjust_marker) {
        return -EAGAIN;
      }

      if (lost_lock) {
        return -EBUSY;
      }
    }
    return 0;
  }
    

  int incremental_sync() {
    reenter(&incremental_cr) {
      set_status("incremental_sync");
      can_adjust_marker = true;
      /* grab lock */
      if (!lease_cr) { /* could have had  a lease_cr lock from previous state */
        yield {
          uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
          string lock_name = "sync_lock";
          RGWRados *store = sync_env->store;
          lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                              sync_env->shard_obj_name(shard_id),
                                              lock_name, lock_duration, this);
          lease_cr->get();
          spawn(lease_cr, false);
          lost_lock = false;
        }
        while (!lease_cr->is_locked()) {
          if (lease_cr->is_done()) {
            ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
            drain_all();
            return lease_cr->get_ret_status();
          }
          set_sleeping(true);
          yield;
        }
        *reset_backoff = true;
      }
      mdlog_marker = sync_marker.marker;
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(sync_env,
                                                         sync_env->shard_obj_name(shard_id),
                                                         sync_marker));

      /*
       * mdlog_marker: the remote sync marker positiion
       * sync_marker: the local sync marker position
       * max_marker: the max mdlog position that we fetched
       * marker: the current position we try to sync
       */
      marker = max_marker = sync_marker.marker;
      /* inc sync */
      do {
        if (!lease_cr->is_locked()) {
          lost_lock = true;
          break;
        }
#define INCREMENTAL_MAX_ENTRIES 100
	ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker <= max_marker) {
	  /* we're at the tip, try to bring more entries */
          ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " syncing mdlog for shard_id=" << shard_id << dendl;
	  yield call(new RGWCloneMetaLogCoroutine(sync_env, shard_id, mdlog_marker, &mdlog_marker));
	}
        if (retcode < 0) {
          ldout(sync_env->cct, 10) << *this << ": failed to fetch more log entries, retcode=" << retcode << dendl;
          yield lease_cr->go_down();
          drain_all();
          return retcode;
        }
	ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker > max_marker) {
          marker = max_marker;
          yield call(new RGWReadMDLogEntriesCR(sync_env, shard_id, &max_marker, INCREMENTAL_MAX_ENTRIES, &log_entries, &truncated));
          for (log_iter = log_entries.begin(); log_iter != log_entries.end(); ++log_iter) {
            if (!mdlog_entry.convert_from(*log_iter)) {
              ldout(sync_env->cct, 0) << __func__ << ":" << __LINE__ << ": ERROR: failed to convert mdlog entry, shard_id=" << shard_id << " log_entry: " << log_iter->id << ":" << log_iter->section << ":" << log_iter->name << ":" << log_iter->timestamp << " ... skipping entry" << dendl;
              continue;
            }
            ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " log_entry: " << log_iter->id << ":" << log_iter->section << ":" << log_iter->name << ":" << log_iter->timestamp << dendl;
            if (!marker_tracker->start(log_iter->id, 0, log_iter->timestamp)) {
              ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << log_iter->id << ". Duplicate entry?" << dendl;
            } else {
              raw_key = log_iter->section + ":" + log_iter->name;
              yield {
                RGWCoroutinesStack *stack = spawn(new RGWMetaSyncSingleEntryCR(sync_env, raw_key, log_iter->id, mdlog_entry.log_data.status, marker_tracker), false);
                assert(stack);
                stack->get();

                stack_to_pos[stack] = log_iter->id;
                pos_to_prev[log_iter->id] = marker;
              }
            }
            marker = log_iter->id;
          }
        }
        collect_children();
	ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " max_marker=" << max_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker == max_marker && can_adjust_marker) {
#define INCREMENTAL_INTERVAL 20
	  yield wait(utime_t(INCREMENTAL_INTERVAL, 0));
	}
      } while (can_adjust_marker);

      while (num_spawned() > 1) {
        yield wait_for_child();
        collect_children();
      }

      yield lease_cr->go_down();

      drain_all();

      if (lost_lock) {
        return -EBUSY;
      }

      if (!can_adjust_marker) {
        return -EAGAIN;
      }
    }
    /* TODO */
    return 0;
  }
};

class RGWMetaSyncShardControlCR : public RGWBackoffControlCR
{
  RGWMetaSyncEnv *sync_env;

  rgw_bucket pool;

  uint32_t shard_id;
  rgw_meta_sync_marker sync_marker;

  RGWObjectCtx obj_ctx;

public:
  RGWMetaSyncShardControlCR(RGWMetaSyncEnv *_sync_env,
		     rgw_bucket& _pool,
		     uint32_t _shard_id,
                     rgw_meta_sync_marker& _marker) : RGWBackoffControlCR(_sync_env->cct), sync_env(_sync_env),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker), obj_ctx(sync_env->store) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", shard_id);
  }

  RGWCoroutine *alloc_cr() {
    return new RGWMetaSyncShardCR(sync_env, pool, shard_id, sync_marker, backoff_ptr());
  }

  RGWCoroutine *alloc_finisher_cr() {
    RGWRados *store = sync_env->store;
    return new RGWSimpleRadosReadCR<rgw_meta_sync_marker>(sync_env->async_rados, store, obj_ctx, store->get_zone_params().log_pool,
                                                               sync_env->shard_obj_name(shard_id), &sync_marker);
  }
};

class RGWMetaSyncCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  rgw_meta_sync_status sync_status;

  map<int, RGWMetaSyncShardControlCR *> shard_crs;


public:
  RGWMetaSyncCR(RGWMetaSyncEnv *_sync_env, rgw_meta_sync_status& _sync_status) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
						      sync_status(_sync_status) {
  }

  int operate() {
    reenter(this) {
      yield {
	map<uint32_t, rgw_meta_sync_marker>::iterator iter = sync_status.sync_markers.begin();
	for (; iter != sync_status.sync_markers.end(); ++iter) {
	  uint32_t shard_id = iter->first;
	  rgw_meta_sync_marker marker;

	  RGWMetaSyncShardControlCR *shard_cr = new RGWMetaSyncShardControlCR(sync_env,
                                                                              sync_env->store->get_zone_params().log_pool,
                                                                              shard_id,
                                                                              sync_status.sync_markers[shard_id]);


	  shard_crs[shard_id] = shard_cr;
          spawn(shard_cr, true);
        }
      }
      yield return set_cr_done();
    }
    return 0;
  }

  void wakeup(int shard_id) {
    map<int, RGWMetaSyncShardControlCR *>::iterator iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second->wakeup();
  }
};

void RGWRemoteMetaLog::init_sync_env(RGWMetaSyncEnv *env) {
  env->cct = store->ctx();
  env->store = store;
  env->conn = conn;
  env->async_rados = async_rados;
  env->http_manager = &http_manager;
  env->error_logger = error_logger;
}

int RGWRemoteMetaLog::clone_shards(int num_shards, vector<string>& clone_markers)
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)num_shards; i++) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
    stack->call(new RGWCloneMetaLogCoroutine(&sync_env, i, clone_markers[i], NULL));

    stacks.push_back(stack);
  }

  return run(stacks);
}

int RGWRemoteMetaLog::fetch(int num_shards, vector<string>& clone_markers)
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)num_shards; i++) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
    stack->call(new RGWCloneMetaLogCoroutine(&sync_env, i, clone_markers[i], NULL));

    stacks.push_back(stack);
  }

  return run(stacks);
}

int RGWRemoteMetaLog::read_sync_status(rgw_meta_sync_status *sync_status)
{
  if (store->is_meta_master()) {
    return 0;
  }

  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWReadSyncStatusCoroutine(&sync_env, obj_ctx, sync_status));
}

int RGWRemoteMetaLog::init_sync_status(int num_shards)
{
  if (store->is_meta_master()) {
    return 0;
  }

  if (!num_shards) {
    rgw_mdlog_info mdlog_info;
    int r = read_log_info(&mdlog_info);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: fail to fetch master log info (r=" << r << ")" << dendl;
      return r;
    }
    num_shards = mdlog_info.num_shards;
  }

  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWInitSyncStatusCoroutine(&sync_env, obj_ctx, num_shards));
}

int RGWRemoteMetaLog::set_sync_info(const rgw_meta_sync_info& sync_info)
{
  return run(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 sync_env.status_oid(), sync_info));
}

int RGWRemoteMetaLog::run_sync(int num_shards, rgw_meta_sync_status& sync_status)
{
  if (store->is_meta_master()) {
    return 0;
  }

  RGWObjectCtx obj_ctx(store, NULL);

  rgw_mdlog_info mdlog_info;
  int r = read_log_info(&mdlog_info);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: fail to fetch master log info (r=" << r << ")" << dendl;
    return r;
  }

  if (!num_shards) {
    num_shards = mdlog_info.num_shards;
  } else if ((uint32_t)num_shards != mdlog_info.num_shards) {
    lderr(store->ctx()) << "ERROR: can't sync, mismatch between num shards, master num_shards=" << mdlog_info.num_shards << " local num_shards=" << num_shards << dendl;
    return r;
  }

  do {
    r = run(new RGWReadSyncStatusCoroutine(&sync_env, obj_ctx, &sync_status));
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status r=" << r << dendl;
      return r;
    }

    if (sync_status.sync_info.state == rgw_meta_sync_info::StateInit) {
      ldout(store->ctx(), 20) << __func__ << "(): init" << dendl;
      r = run(new RGWInitSyncStatusCoroutine(&sync_env, obj_ctx, num_shards));
      if (r == -EBUSY) {
        backoff.backoff_sleep();
        continue;
      }
      backoff.reset();
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to init sync status r=" << r << dendl;
        return r;
      }
    }
  } while (sync_status.sync_info.state == rgw_meta_sync_info::StateInit);

  do {
    r = run(new RGWReadSyncStatusCoroutine(&sync_env, obj_ctx, &sync_status));
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status r=" << r << dendl;
      return r;
    }

    switch ((rgw_meta_sync_info::SyncState)sync_status.sync_info.state) {
      case rgw_meta_sync_info::StateBuildingFullSyncMaps:
        ldout(store->ctx(), 20) << __func__ << "(): building full sync maps" << dendl;
        r = run(new RGWFetchAllMetaCR(&sync_env, num_shards, sync_status.sync_markers));
        if (r == -EBUSY || r == -EAGAIN) {
          backoff.backoff_sleep();
          continue;
        }
        backoff.reset();
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to fetch all metadata keys" << dendl;
          return r;
        }

        sync_status.sync_info.state = rgw_meta_sync_info::StateSync;
        r = set_sync_info(sync_status.sync_info);
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to update sync status" << dendl;
          return r;
        }
        /* fall through */
      case rgw_meta_sync_info::StateSync:
        ldout(store->ctx(), 20) << __func__ << "(): sync" << dendl;
        meta_sync_cr = new RGWMetaSyncCR(&sync_env, sync_status);
        r = run(meta_sync_cr);
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to fetch all metadata keys" << dendl;
          return r;
        }
        break;
      default:
        ldout(store->ctx(), 0) << "ERROR: bad sync state!" << dendl;
        return -EIO;
    }
  } while (!going_down.read());

  return 0;
}

void RGWRemoteMetaLog::wakeup(int shard_id)
{
  if (!meta_sync_cr) {
    return;
  }
  meta_sync_cr->wakeup(shard_id);
}

int RGWCloneMetaLogCoroutine::operate()
{
  reenter(this) {
    do {
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": init request" << dendl;
        return state_init();
      }
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status" << dendl;
        return state_read_shard_status();
      }
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status complete" << dendl;
        return state_read_shard_status_complete();
      }
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": sending rest request" << dendl;
        return state_send_rest_request();
      }
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": receiving rest response" << dendl;
        return state_receive_rest_response();
      }
      yield {
        ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries" << dendl;
        return state_store_mdlog_entries();
      }
    } while (truncated);
    yield {
      ldout(cct, 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries complete" << dendl;
      return state_store_mdlog_entries_complete();
    }
  }

  return 0;
}

int RGWCloneMetaLogCoroutine::state_init()
{
  data = rgw_mdlog_shard_data();

  return 0;
}

int RGWCloneMetaLogCoroutine::state_read_shard_status()
{
  int ret = mdlog->get_info_async(shard_id, &shard_info, stack->get_completion_mgr(), (void *)stack, &req_ret);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
    return set_cr_error(ret);
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_read_shard_status_complete()
{
  ldout(cct, 20) << "shard_id=" << shard_id << " marker=" << shard_info.marker << " last_update=" << shard_info.last_update << dendl;

  marker = shard_info.marker;

  return 0;
}

int RGWCloneMetaLogCoroutine::state_send_rest_request()
{
  RGWRESTConn *conn = sync_env->conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  char max_entries_buf[32];
  snprintf(max_entries_buf, sizeof(max_entries_buf), "%d", max_entries);

  const char *marker_key = (marker.empty() ? "" : "marker");

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { "max-entries", max_entries_buf },
                                  { marker_key, marker.c_str() },
                                  { NULL, NULL } };

  http_op = new RGWRESTReadResource(conn, "/admin/log", pairs, NULL, sync_env->http_manager);

  http_op->set_user_info((void *)stack);

  int ret = http_op->aio_read();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch mdlog data" << dendl;
    log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
    http_op->put();
    http_op = NULL;
    return ret;
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_receive_rest_response()
{
  int ret = http_op->wait(&data);
  if (ret < 0) {
    error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
    ldout(cct, 0) << "ERROR: failed to wait for op, ret=" << ret << dendl;
    http_op->put();
    http_op = NULL;
    return set_cr_error(ret);
  }
  http_op->put();
  http_op = NULL;

  ldout(cct, 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  truncated = ((int)data.entries.size() == max_entries);

  if (data.entries.empty()) {
    if (new_marker) {
      *new_marker = marker;
    }
    return set_cr_done();
  }

  if (new_marker) {
    *new_marker = data.entries.back().id;
  }

  return 0;
}


int RGWCloneMetaLogCoroutine::state_store_mdlog_entries()
{
  list<cls_log_entry> dest_entries;

  vector<rgw_mdlog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_mdlog_entry& entry = *iter;
    ldout(cct, 20) << "entry: name=" << entry.name << dendl;

    cls_log_entry dest_entry;
    dest_entry.id = entry.id;
    dest_entry.section = entry.section;
    dest_entry.name = entry.name;
    dest_entry.timestamp = entry.timestamp;
  
    ::encode(entry.log_data, dest_entry.data);

    dest_entries.push_back(dest_entry);

    marker = entry.id;
  }

  RGWAioCompletionNotifier *cn = stack->create_completion_notifier();

  RGWRados *store = sync_env->store;
  int ret = store->meta_mgr->store_md_log_entries(dest_entries, shard_id, cn->completion());
  if (ret < 0) {
    cn->put();
    ldout(cct, 10) << "failed to store md log entries shard_id=" << shard_id << " ret=" << ret << dendl;
    return set_cr_error(ret);
  }
  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_store_mdlog_entries_complete()
{
  return set_cr_done();
}


