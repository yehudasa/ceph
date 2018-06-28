#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_op.h"
#include "rgw_pubsub.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


#define PS_NUM_PUB_SHARDS_DEFAULT 64
#define PS_NUM_PUB_SHARDS_MIN     16

#define PS_NUM_TOPIC_SHARDS_DEFAULT 16
#define PS_NUM_TOPIC_SHARDS_MIN     8

struct PSSubConfig { /* subscription config */
  string name;
  string topic;
  string push_endpoint;

  void init(CephContext *cct, const JSONFormattable& config) {
    name = config["name"];
    topic = config["topic"];
    push_endpoint = config["push_endpoint"];
  }
};

struct PSTopicConfig {
  string name;
};

struct PSNotificationConfig {
  string path; /* a path or a path prefix that would trigger the event (prefix: if ends with a wildcard) */
  string topic;

  uint64_t id{0};
  bool is_prefix{false};

  void init(CephContext *cct, const JSONFormattable& config) {
    path = config["path"];
    if (!path.empty() && path[path.size() - 1] == '*') {
      path = path.substr(0, path.size() - 1);
      is_prefix = true;
    }
    topic = config["topic"];
  }
};


struct PSConfig {
  string id{"pubsub"};
  uint64_t sync_instance{0};
  uint32_t num_pub_shards{0};
  uint32_t num_topic_shards{0};
  uint64_t max_id{0};

  /* FIXME: no hard coded buckets, we'll have configurable topics */
  vector<PSSubConfig> subscriptions;
  map<string, PSTopicConfig> topics;
  multimap<string, PSNotificationConfig> notifications;

  void init(CephContext *cct, const JSONFormattable& config) {
    num_pub_shards = config["num_pub_shards"](PS_NUM_PUB_SHARDS_DEFAULT);
    if (num_pub_shards < PS_NUM_PUB_SHARDS_MIN) {
      num_pub_shards = PS_NUM_PUB_SHARDS_MIN;
    }

    num_topic_shards = config["num_topic_shards"](PS_NUM_TOPIC_SHARDS_DEFAULT);
    if (num_topic_shards < PS_NUM_TOPIC_SHARDS_MIN) {
      num_topic_shards = PS_NUM_TOPIC_SHARDS_MIN;
    }
    /* FIXME: this will be dynamically configured */
    for (auto& c : config["notifications"].array()) {
      PSNotificationConfig nc;
      nc.id = ++max_id;
      nc.init(cct, c);
      notifications.insert(std::make_pair(nc.path, nc));

      PSTopicConfig topic_config = { .name = nc.topic };
      topics[nc.topic] = topic_config;
    }
    for (auto& c : config["subscriptions"].array()) {
      PSSubConfig sc;
      sc.init(cct, c);
      subscriptions.push_back(sc);
    }
  }

  void init_instance(RGWRealm& realm, uint64_t instance_id) {
    sync_instance = instance_id;
  }

  void get_notifs(const RGWBucketInfo& bucket_info, const rgw_obj_key& key, vector<PSNotificationConfig *> *notifs) {
    string path = bucket_info.bucket.name + "/" + key.name;

    notifs->clear();

    auto iter = notifications.upper_bound(path);
    if (iter == notifications.begin()) {
      return;
    }

    --iter;
    do {
      if (iter->first.size() > path.size()) {
        break;
      }
      if (path.compare(0, iter->first.size(), iter->first) != 0) {
        break;
      }

      PSNotificationConfig *target = &iter->second;

      if (!target->is_prefix &&
          path.size() != iter->first.size()) {
        continue;
      }

      notifs->push_back(target);
    } while (iter != notifications.begin());
  }
};

using PSConfigRef = std::shared_ptr<PSConfig>;

class RGWPSInitConfigCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSConfigRef conf;
public:
  RGWPSInitConfigCBCR(RGWDataSyncEnv *_sync_env,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                                    sync_env(_sync_env),
                                                    conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

      /* nothing to do here right now */

      return set_cr_done();
    }
    return 0;
  }
};


/*
 * scaling timelog cr
 */

struct slog_part {
  string key; /* key in meta log for this part's entry */
  uint64_t id; /* id of part, used for part oid */

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(slog_part)


struct slog_meta_entry {
  ceph::real_time timestamp;
  slog_part part;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(timestamp, bl);
    encode(part, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(timestamp, bl);
    decode(part, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(slog_meta_entry)

class ScalingTimelog {
  RGWRados *store;

  string subsystem;
  string name;

  slog_part cur_part;
  atomic<int> cur_part_id;

  atomic<long long> counter;

public:
  ScalingTimelog(RGWRados *_store, const string& _subsystem, const string& _name) :
    store(_store), subsystem(_subsystem), name(_name) {}

  string meta_oid() {
    char buf[64 + subsystem.size() + name.size()];
    snprintf(buf, sizeof(buf), "stimelog.%s.meta/%s", subsystem.c_str(), name.c_str());
    return string(buf);
  }

  int get_cur_part_id() {
    return cur_part_id;
  }

  string part_oid(uint64_t index) {
    char buf[64 + subsystem.size() + name.size()];
    snprintf(buf, sizeof(buf), "stimelog.%s/%s.%lld", subsystem.c_str(), name.c_str(), (long long)index);
    return string(buf);
  }

  void prepare_meta_entry(int part_id, cls_log_entry *entry) {
    string section; /* unused */
    string name; /* unused */

    slog_meta_entry info;

    info.timestamp = real_clock::now();
  
    bufferlist bl;
    encode(info, bl);
    store->time_log_prepare_entry(*entry, info.timestamp, section, name, bl);

    generate_meta_entry_id(part_id, &entry->id);
  }

  int decode_meta_entry(const cls_log_entry& entry, slog_meta_entry *info) {
    auto iter = entry.data.begin();

    try {
      decode(*info, iter);
    } catch (buffer::error& err) {
      return -EIO;
    }
    
    return 0;
  }

  template <class T>
  void encode_log_entry(const string& section, const string& name, cls_log_entry *entry) {
    bufferlist bl;
    T info;
    encode(info, bl);

    ceph::real_time timestamp = real_clock::now();

    store->time_log_prepare_entry(*entry, timestamp, section, name, bl);

    generate_entry_id(timestamp, &entry->id);
  }

  template <class T>
  int decode_log_entry(const cls_log_entry& entry,
                        T *info) {
    auto iter = entry.data.begin();
    try {
      decode(*info, iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): failed to decode entry" << dendl;
      return -EIO;
    }

    return 0;
  }

  void registered_part(const slog_part& new_cur_part) {
    cur_part = new_cur_part;
  }

  /* internal */
  RGWCoroutine *get_cur_part_cr(int *pcur_part);
  RGWCoroutine *register_part_cr(int part);

  void generate_meta_entry_id(int part_id, string *id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "1_%06d", part_id);

    *id = buf;
  }

  void generate_entry_id(const ceph::real_time& timestamp, string *id) {
    utime_t ts(timestamp);
    char buf[64];
    snprintf(buf, sizeof(buf), "1_%010ld.%06ld_%lld", (long)ts.sec(), (long)ts.usec(), ++counter); /* log_index_prefix = "1_" */

    *id = buf;
  }

  void get_slog_id(int part_num, const string& entry_id, string *slog_id) {
    char buf[16 + entry_id.size()];
    snprintf(buf, sizeof(buf), "%d:%s", part_num, entry_id.c_str());
    *slog_id = buf;
  }

  void get_slog_id(int part_num, const cls_log_entry& entry, string *slog_id) {
    get_slog_id(part_num, entry.id, slog_id);
  }

  int parse_slog_id(const string& slog_id, int *part_id, string *entry_id) {
    size_t pos = slog_id.find(":");
    if (pos == string::npos ||
        pos >= slog_id.size()) {
      return -EINVAL;
    }

    string first = slog_id.substr(0, pos);
    *entry_id = slog_id.substr(pos + 1);

    string err;
    *part_id = (int)strict_strtoll(first.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(store->ctx(), 20) << "bad slog id: " << slog_id << ": failed to parse: " << err << dendl;
      return -EINVAL;
    }

    return 0;
  }

  template <class T>
  struct list_result {
    string key;
    string section;
    string name;
    T entry;
  };

  RGWCoroutine *list_parts_cr(const slog_part& part_marker, int max_parts, deque<slog_meta_entry> *parts, bool *truncated);


  /* external */
  RGWCoroutine *init_cr();

  template <class T>
  RGWCoroutine *log_entry_cr(const string& section, const string& name, T& info, string *slog_id);

  template <class T>
  RGWCoroutine *get_entry_cr(const string& slog_id, string *section, string *name, T *info);

  template <class T>
  RGWCoroutine *list_entries_cr(const string& marker, int max_entries,
                                vector<list_result<T> > *result,
                                string *end_marker,
                                bool *truncated);
};


class ScalingTimelogGetCurPartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  slog_part *cur_part;

  RGWRados *store;
  cls_log_header log_header;
  cls_log_entry entry;
  slog_meta_entry meta_info;
public:
  ScalingTimelogGetCurPartCR(RGWDataSyncEnv *_sync_env,
                           ScalingTimelog *_slog,
                           slog_part *_cur_part) : RGWCoroutine(_sync_env->cct),
                                       sync_env(_sync_env),
                                       slog(_slog),
                                       cur_part(_cur_part) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {

      yield call (new RGWRadosTimelogInfoCR(store,
                                            slog->meta_oid(),
                                            &log_header));
      if (retcode < 0) {
        if (retcode != -ENOENT) {
          ldout(store->ctx(), 0) << "ERROR: failed to read timelog header: oid=" <<  slog->meta_oid() << " ret=" << retcode << dendl;
        }
        return set_cr_error(retcode);
      }

      yield call (new RGWRadosTimelogGetCR(store,
                                         slog->meta_oid(),
                                         log_header.max_marker,
                                         &entry));

      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to read timelog entry: oid=" <<  slog->meta_oid() << " key=" << log_header.max_marker << " ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      retcode = slog->decode_meta_entry(entry, &meta_info);
      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to decode read timelog entry: oid=" <<  slog->meta_oid() << " key=" << log_header.max_marker << " ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      cur_part->key = entry.id;
      cur_part->id = meta_info.part.id;

      return set_cr_done();
    }
    return 0;
  }
};

class ScalingTimelogRegisterPartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  int part_id;
  slog_part cur_part;

  RGWRados *store;
  cls_log_header log_header;
public:
  ScalingTimelogRegisterPartCR(RGWDataSyncEnv *_sync_env,
                     ScalingTimelog *_slog,
                     int _part_id) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               slog(_slog),
                                               part_id(_part_id) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      yield {
        cls_log_entry entry;

        slog->prepare_meta_entry(part_id, &entry);
        cur_part.key = entry.id;
        cur_part.id = part_id;

        call (new RGWRadosTimelogAddCR(store, slog->meta_oid(), entry));
      }
      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to set timelog entry: oid=" << slog->meta_oid() << " ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      slog->registered_part(cur_part);

      return set_cr_done();
    }
    return 0;
  }
};

class ScalingTimelogInitCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  int *cur_part;

  RGWRados *store;
public:
  ScalingTimelogInitCR(RGWDataSyncEnv *_sync_env,
                     ScalingTimelog *_slog,
                     int *_cur_part,
                     bool *_pinitialied) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               slog(_slog),
                                               cur_part(_cur_part) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      yield call (slog->get_cur_part_cr(cur_part));
      if (retcode < 0 && retcode != -ENOENT) {
        return set_cr_error(retcode);
      }

      if (retcode >= 0) {
        return set_cr_done();
      }

      yield call(slog->register_part_cr(1));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      *cur_part = 0;

      return set_cr_done();
    }
    return 0;
  }
};

class ScalingTimelogListPartsCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  slog_part part_marker;
  int max_entries;
  deque<slog_meta_entry> *result;
  bool *truncated;

  list<cls_log_entry> entries;

  RGWRados *store;
  ceph::real_time start_time;
  ceph::real_time end_time;

public:
  ScalingTimelogListPartsCR(RGWDataSyncEnv *_sync_env,
                           ScalingTimelog *_slog,
                           const slog_part& _part_marker,
                           int _max_entries,
                           deque<slog_meta_entry> *_result,
                           bool *_truncated) : RGWCoroutine(_sync_env->cct),
                                       sync_env(_sync_env),
                                       slog(_slog),
                                       part_marker(_part_marker),
                                       max_entries(_max_entries),
                                       result(_result),
                                       truncated(_truncated) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      result->clear();

      yield call(new RGWRadosTimelogListCR(store,
                                           slog->meta_oid(),
                                           start_time, end_time,
                                           part_marker.key, /* marker */
                                           max_entries,
                                           &entries,
                                           nullptr,
                                           truncated));

      if (retcode < 0 && retcode != -ENOENT) {
        ldout(store->ctx(), 0) << "ERROR: failed to list timelog entries: oid=" <<  slog->meta_oid() << " marker=" << part_marker.key << " ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      if (retcode == -ENOENT) {
        *truncated = false;
        return set_cr_done();
      }

      for (auto& entry : entries) {
        slog_meta_entry meta_info;
        retcode = slog->decode_meta_entry(entry, &meta_info);
        if (retcode < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to decode read timelog entry: oid=" <<  slog->meta_oid() << " key=" << entry.id << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        result->push_back(meta_info);
      }

      return set_cr_done();
    }
    return 0;
  }
};

template <class T>
class ScalingTimelogLogEntryCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  string section;
  string name;
  T info;
  cls_log_entry entry;
  int cur_part;
  string *slog_id;

  RGWRados *store;
  int i;
public:
  ScalingTimelogLogEntryCR(RGWDataSyncEnv *_sync_env,
                     ScalingTimelog *_slog,
                     const string& _section,
                     const string& _name,
                     const T& _info,
                     string *_slog_id) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               slog(_slog),
                                               section(_section),
                                               name(_name),
                                               info(_info),
                                               slog_id(_slog_id) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      retcode = slog->encode_log_entry(section, name, info, &entry);
      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to encode log entry" << dendl;
        return set_cr_error(retcode);
      }
#define MAX_RACE_LOOP 10
      for (i = 0; i < MAX_RACE_LOOP; i++) {
        cur_part = slog->get_cur_part_id();
        yield call (new RGWRadosTimelogAddCR(store, slog->part_oid(cur_part), entry));
        if (retcode >= 0) {
          break;
        }

        if (retcode == -ENOSPC) {
          ++cur_part;
          yield call(slog->register_part_cr(cur_part));
          if (retcode < 0) {
            ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): failed to register a new log part ret=" << retcode << dendl;
            return set_cr_error(retcode);
          }

          continue;
        }
      }
      if (i == MAX_RACE_LOOP) {
        ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): too many iterations, probably a bug!" << dendl;
        return -EIO;
      }

      slog->get_slog_id(cur_part, entry, slog_id);

      return set_cr_done();
    }
    return 0;
  }
};

template <class T>
class ScalingTimelogGetEntryCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  string slog_id;

  int part_id;
  string entry_id;

  string *section;
  string *name;
  T *info;

  RGWRados *store;

  cls_log_entry entry;
public:
  ScalingTimelogGetEntryCR(RGWDataSyncEnv *_sync_env,
                     ScalingTimelog *_slog,
                     const string& _slog_id,
                     string *_section,
                     string *_name,
                     T *_info) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               slog(_slog),
                                               slog_id(_slog_id),
                                               section(_section),
                                               name(_name),
                                               info(_info) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      retcode = slog->parse_slog_id(slog_id, &part_id, &entry_id);

      yield {
        call(new RGWRadosTimelogGetCR(store, slog->part_oid(part_id), entry_id, &entry));
      }
      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to read timelog entry: oid=" << slog->part_oid(part_id) << " entry_id=" << entry_id << " ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      *section = entry.section;
      *name = entry.name;
      slog->decode_log_entry(entry, info);

      return set_cr_done();
    }
    return 0;
  }
};

template <class T>
class ScalingTimelogListEntriesCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ScalingTimelog *slog;
  string marker;
  int max_entries;

  vector<ScalingTimelog::list_result<T> > *result;
  string *out_marker;
  bool *ptruncated;

  int cur_part;
  string part_marker;
  bool truncated;
  list<cls_log_entry> entries;
  string meta_marker;

  deque<slog_meta_entry> next_parts;

  RGWRados *store;
public:
  ScalingTimelogListEntriesCR(RGWDataSyncEnv *_sync_env,
                     ScalingTimelog *_slog,
                     const string& _marker,
                     int _max_entries,
                     vector<ScalingTimelog::list_result<T> > *_result,
                     string *_out_marker,
                     bool *_ptruncated) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               slog(_slog),
                                               marker(_marker),
                                               max_entries(_max_entries),
                                               result(_result) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {
      ceph::real_time start_time;
      ceph::real_time end_time;

      if (!marker.empty()) {
        retcode = slog->parse_slog_id(marker, &cur_part, &part_marker);
        if (retcode < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to parse marker: marker=" << marker << dendl;
          return set_cr_error(retcode);
        }
      }

      do {
        entries.clear();

        yield call (new RGWRadosTimelogListCR(store,
                                              slog->part_oid(cur_part),
                                              start_time, end_time,
                                              max_entries,
                                              entries,
                                              part_marker,
                                              &part_marker,
                                              &truncated));

        if (retcode == -ENOENT) {
          retcode = 0;
          truncated = false;
        }
        if (retcode < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to list timelog entries: oid=" <<  slog->part_oid(cur_part) << " marker=" << part_marker << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        for (auto& entry : entries) {
          ScalingTimelog::list_result<T> e;
          retcode = slog->decode_log_entry(entry, &e.entry);
          if (retcode < 0) {
            ldout(store->ctx(), 0) << "ERROR: failed to decode read timelog entry: oid=" <<  slog->part_oid(cur_part) << " key=" << entry.id << " ret=" << retcode << dendl;
            return set_cr_error(retcode);
          }

          e.key = entry.id;
          e.section = entry.section;
          e.name = entry.name;

          result->push_back(e);
        }

        max_entries -= entries.size();

        if (!truncated) {
          if (next_parts.empty()) {
            slog->generate_meta_entry_id(cur_part, &meta_marker);
#define NUM_PARTS 16
            yield call(slog->list_parts_cr(meta_marker, 16, &next_parts, &truncated));
            if (retcode > 0) {
              ldout(store->ctx(), 0) << "ERROR: failed to fetch next parts: retcode=" << retcode << dendl;
            }
            if (next_parts.empty()) {
              /* we read everything! */
              *ptruncated = false;
              return set_cr_done();
            }
          }
          slog_part next_part = next_parts.front();
          next_parts.pop_front();
          cur_part = next_part.id;
          part_marker.clear();
        }
      } while (max_entries > 0);

      *truncated = true; /* maybe */
      slog->get_slog_id(cur_part, part_marker, out_marker);

      return set_cr_done();
    }
    return 0;
  }
};


RGWCoroutine ScalingTimelog::list_parts_cr(const slog_part& part_marker, int max_parts, deque<slog_meta_entry> *parts, bool *truncated)
{
  return new ScalingTimelogListPartsCR(sync_env, this, part_marker, max_parts, parts, truncated);
}

RGWCoroutine ScalingTimelog::get_cur_part_cr(int *pcur_part)
{
  return new ScalingTimelogGetCurPartCR(sync_env, this, pcur_part);
}

RGWCoroutine *register_part_cr(int part_id)
{
  return new ScalingTimelogRegisterPartCR(sync_env, this, part_id);
}

RGWCoroutine ScalingTimelog::init_cr()
{
  return new ScalingTimelogInitCR(sync_env, this, &cur_part);
}

template <class T>
RGWCoroutine *ScalingTimelog::log_entry_cr(const string& section, const string& name, const T& info, string *slog_id)
{
  return new ScalingTimelogLogEntryCR(sync_env, this, section, name, info, slog_id);
}

template <class T>
RGWCoroutine *ScalingTimelog::get_entry_cr(const string& slog_id, string *section, string *name, T *info)
{
  return new ScalingTimelogGetEntryCR(sync_env, this, slog_id, section, name, info);
}

template <class T>
RGWCoroutine *ScalingTimelog::list_entries_cr(const string& marker, int max_entries,
                                              vector<list_result<T> > *result,
                                              string *end_marker,
                                              bool *truncated)
{
  return new ScalingTimelogListEntriesCR(sync_env, this, marker,
                                       max_entries, result,
                                       end_marker, truncated);
}

class PSTopicWriteCurIndexMeta : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  PSConfigRef conf;
  string meta_oid;
public:
  PSTopicShardPrepare(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     int _shard_id,
                     PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               conf(_conf) {
  }

  int operate() override {
    reenter(this) {

      return set_cr_done();
    }
    return 0;
  }
};



class PSTopicShardGetCurIndexMeta : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  PSConfigRef conf;
  string meta_oid;
  cls_log_header log_header;
  RGWRados *store;
public:
  PSTopicShardGetCurIndex(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     int _shard_id,
                     PSConfigRef _conf,
                     int *cur_index) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               conf(_conf) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {

      yield {
        call (new RGWRadosTimelogInfoCR(store,
                                        rgw_raw_obj(store->get_zone_params().log_pool, topic_shard_meta_oid(topic, shard_id)),
                                        &log_header));
      }
      if (retcode < 0 && ret != -ENOENT) {
        return set_cr_error(retcode);
      }

      if (ret != -ENOENT) {
        yield {
          cls_log_entry entry;

          prepare_timelog_entry(0, &entry);

          call (new RGWRadosTimelogAddCR(store,
                                          rgw_raw_obj(store->get_zone_params().log_pool, topic_shard_meta_oid(topic, shard_id)),
                                          entry));
        }

        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        *cur_index = 0;

        return set_cr_done();
      }

     ... 

      return set_cr_done();
    }
    return 0;
  }
};

class PSTopicShardAddLogEntry : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  std::shared_ptr<rgw_pubsub_event> event;
  PSConfigRef conf;
  string meta_oid;
public:
  PSTopicShardAddLogEntry(RGWDataSyncEnv *_sync_env,
                          const string& _topic,
                          int _shard_id,
                          std::shared_ptr<rgw_pubsub_event>& _event,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               event(_event),
                                               conf(_conf) {
  }

  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

#warning implement me      

      return set_cr_done();
    }
    return 0;
  }
};

class PSTopicAddLogEntry : public RGWCoroutine {
  static std::atomic<int> counter;
  RGWDataSyncEnv *sync_env;
  string topic;
  std::shared_ptr<rgw_pubsub_event> event;
  PSConfigRef conf;
public:
  PSTopicAddLogEntry(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     std::shared_ptr<rgw_pubsub_event>& _event,
                     PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               event(_event),
                                               conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 20) << "PSTopicAddLogEntry: " << sync_env->source_zone << dendl;

      
      yield {
        int shard_id = ++counter % conf->num_topic_shards;
        call(new PSTopicShardAddLogEntry(sync_env, topic, shard_id, event, conf));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: PSTopicShardAddLogEntry() returned " << retcode << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  PSConfigRef conf;
  uint64_t versioned_epoch;
  vector<PSNotificationConfig *> notifs;
  vector<PSNotificationConfig *>::iterator niter;
public:
  RGWPSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          PSConfigRef _conf, uint64_t _versioned_epoch) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf),
                                                                               versioned_epoch(_versioned_epoch) {
#warning this will need to change obviously
    conf->get_notifs(_bucket_info, _key, &notifs);
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": stat of remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                               << " attrs=" << attrs << dendl;


      for (niter = notifs.begin(); niter != notifs.end(); ++niter) {
        yield {
          ldout(sync_env->cct, 10) << ": notification for " << bucket_info.bucket << "/" << key << ": id=" << (*niter)->id << " path=" << (*niter)->path << ", topic=" << (*niter)->topic << dendl;

#warning publish notification
#if 0
        string path = conf->get_obj_path(bucket_info, key);
        es_obj_metadata doc(sync_env->cct, conf, bucket_info, key, mtime, size, attrs, versioned_epoch);

        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf->conn.get(),
                                                            sync_env->http_manager,
                                                            path, nullptr /* params */,
                                                            doc, nullptr /* result */));
#endif
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  PSConfigRef conf;
  uint64_t versioned_epoch;
public:
  RGWPSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        PSConfigRef _conf, uint64_t _versioned_epoch) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf), versioned_epoch(_versioned_epoch) {
  }

  ~RGWPSHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
#warning things need to change
    /* FIXME: we need to create a pre_callback coroutine that decides whether object should
     * actually be handled. Otherwise we fetch info from remote zone about every object, even
     * if we don't intend to handle it.
     */
    return new RGWPSHandleRemoteObjCBCR(sync_env, bucket_info, key, conf, versioned_epoch);
  }
};

class RGWPSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  PSConfigRef conf;
public:
  RGWPSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": remove remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
#if 0
        string path = conf->get_obj_path(bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf->conn.get(),
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
#endif
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWPSDataSyncModule : public RGWDataSyncModule {
  PSConfigRef conf;
public:
  RGWPSDataSyncModule(CephContext *cct, const JSONFormattable& config) : conf(std::make_shared<PSConfig>()) {
    conf->init(cct, config);
  }
  ~RGWPSDataSyncModule() override {}

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) override {
    conf->init_instance(sync_env->store->get_realm(), instance_id);
  }

  RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": init" << dendl;
    return new RGWPSInitConfigCBCR(sync_env, conf);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSHandleRemoteObjCR(sync_env, bucket_info, key, conf, versioned_epoch);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning requests should be filtered correctly
#if 0
    ldout(sync_env->cct, 10) << conf->id << ": skipping operation (not handled)" << dendl;
#endif
#warning delete markers need to be handled too
    return NULL;
  }
};

RGWPSSyncModuleInstance::RGWPSSyncModuleInstance(CephContext *cct, const JSONFormattable& config)
{
  data_handler = std::unique_ptr<RGWPSDataSyncModule>(new RGWPSDataSyncModule(cct, config));
}

RGWDataSyncModule *RGWPSSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTMgr *RGWPSSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
#warning REST filter implementation missing
#if 0
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  delete orig;
  return new RGWRESTMgr_MDSearch_S3();
#endif
  return orig;
}

int RGWPSSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
  instance->reset(new RGWPSSyncModuleInstance(cct, config));
  return 0;
}

