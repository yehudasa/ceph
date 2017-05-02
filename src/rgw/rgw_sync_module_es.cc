#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_es.h"
#include "rgw_sync_module_es_rest.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_op.h"
#include "rgw_es_query.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw


/*
 * whitelist utility. Config string is a list of entries, where an entry is either an item,
 * a prefix, or a suffix. An item would be the name of the entity that we'd look up,
 * a prefix would be a string ending with an asterisk, a suffix would be a string starting
 * with an asterisk. For example:
 *
 *      bucket1, bucket2, foo*, *bar
 */
class ItemList {
  bool approve_all{false};

  set<string> entries;
  set<string> prefixes;
  set<string> suffixes;

  void parse(const string& str) {
    list<string> l;

    get_str_list(str, ",", l);

    for (auto& entry : l) {
      entry = rgw_trim_whitespace(entry);
      if (entry.empty()) {
        continue;
      }

      if (entry == "*") {
        approve_all = true;
        return;
      }

      if (entry[0] == '*') {
        suffixes.insert(entry.substr(1));
        continue;
      }

      if (entry[entry.size() - 1] == '*') {
        prefixes.insert(entry.substr(0, entry.size() - 1));
        continue;
      }

      entries.insert(entry);
    }
  }

public:
  ItemList() {}
  void init(const string& str, bool def_val) {
    if (str.empty()) {
      approve_all = def_val;
    } else {
      parse(str);
    }
  }

  bool exists(const string& entry) {
    if (approve_all) {
      return true;
    }

    if (entries.find(entry) != entries.end()) {
      return true;
    }

    auto i = prefixes.upper_bound(entry);
    if (i != prefixes.begin()) {
      --i;
      if (boost::algorithm::starts_with(entry, *i)) {
        return true;
      }
    }

    for (i = suffixes.begin(); i != suffixes.end(); ++i) {
      if (boost::algorithm::ends_with(entry, *i)) {
        return true;
      }
    }

    return false;
  }
};

struct ElasticConfig {
  string id;
  RGWRESTConn *conn{nullptr};
  bool explicit_custom_meta{true};
  ItemList index_buckets;
  ItemList allow_owners;

  bool should_handle_operation(RGWBucketInfo& bucket_info) {
    return index_buckets.exists(bucket_info.bucket.name) &&
           allow_owners.exists(bucket_info.owner.to_str());
  }
};

using ElasticConfigRef = std::shared_ptr<ElasticConfig>;

static string es_get_index_path(const RGWRealm& realm)
{
  string path = "/rgw-" + realm.get_name();
  return path;
}

static string es_get_obj_path(const RGWRealm& realm, const RGWBucketInfo& bucket_info, const rgw_obj_key& key)
{
  string path = "/rgw-" + realm.get_name() + "/object/" + bucket_info.bucket.bucket_id + ":" + key.name + ":" + key.instance;
  return path;
}

struct es_dump_type {
  const char *type;
  const char *format;

  es_dump_type(const char *t, const char *f = nullptr) : type(t), format(f) {}

  void dump(Formatter *f) const {
    encode_json("type", type, f);
    if (format) {
      encode_json("format", format, f);
    }
  }
};

struct es_index_mappings {
  void dump_custom(Formatter *f, const char *section, const char *type, const char *format) const {
    f->open_object_section(section);
    ::encode_json("type", "nested", f);
    f->open_object_section("properties");
    f->open_object_section("name");
    ::encode_json("type", "string", f);
    ::encode_json("index", "not_analyzed", f);
    f->close_section(); // name
    encode_json("value", es_dump_type(type, format), f);
    f->close_section(); // entry
    f->close_section(); // custom-string
  }
  void dump(Formatter *f) const {
    f->open_object_section("mappings");
    f->open_object_section("object");
    f->open_object_section("properties");
    encode_json("bucket", es_dump_type("string"), f);
    encode_json("name", es_dump_type("string"), f);
    encode_json("instance", es_dump_type("string"), f);
    f->open_object_section("meta");
    f->open_object_section("properties");
    encode_json("cache_control", es_dump_type("string"), f);
    encode_json("content_disposition", es_dump_type("string"), f);
    encode_json("content_encoding", es_dump_type("string"), f);
    encode_json("content_language", es_dump_type("string"), f);
    encode_json("content_type", es_dump_type("string"), f);
    encode_json("etag", es_dump_type("string"), f);
    encode_json("expires", es_dump_type("string"), f);
    f->open_object_section("mtime");
    ::encode_json("type", "date", f);
    ::encode_json("format", "strict_date_optional_time||epoch_millis", f);
    f->close_section(); // mtime
    encode_json("size", es_dump_type("long"), f);
    dump_custom(f, "custom-string", "string", nullptr);
    dump_custom(f, "custom-int", "long", nullptr);
    dump_custom(f, "custom-date", "date", "strict_date_optional_time||epoch_millis");
    f->close_section(); // properties
    f->close_section(); // meta
    f->close_section(); // properties
    f->close_section(); // object
    f->close_section(); // mappings
  }
};

struct es_obj_metadata {
  CephContext *cct;
  ElasticConfigRef es_conf;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  uint64_t size;
  map<string, bufferlist> attrs;

  es_obj_metadata(CephContext *_cct, ElasticConfigRef _es_conf, const RGWBucketInfo& _bucket_info,
                  const rgw_obj_key& _key, ceph::real_time& _mtime, uint64_t _size,
                  map<string, bufferlist>& _attrs) : cct(_cct), es_conf(_es_conf), bucket_info(_bucket_info), key(_key),
                                                     mtime(_mtime), size(_size), attrs(std::move(_attrs)) {}

  void dump(Formatter *f) const {
    map<string, string> out_attrs;
    map<string, string> custom_meta;
    RGWAccessControlPolicy policy;
    set<string> permissions;

    for (auto i : attrs) {
      const string& attr_name = i.first;
      string name;
      bufferlist& val = i.second;

      if (attr_name.compare(0, sizeof(RGW_ATTR_PREFIX) - 1, RGW_ATTR_PREFIX) != 0) {
        continue;
      }

      if (attr_name.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1, RGW_ATTR_META_PREFIX) == 0) {
        name = attr_name.substr(sizeof(RGW_ATTR_META_PREFIX) - 1);
        custom_meta[name] = string(val.c_str(), (val.length() > 0 ? val.length() - 1 : 0));
        continue;
      }

      name = attr_name.substr(sizeof(RGW_ATTR_PREFIX) - 1);

      if (name == "acl") {
        try {
          auto i = val.begin();
          ::decode(policy, i);
        } catch (buffer::error& err) {
          ldout(cct, 0) << "ERROR: failed to decode acl for " << bucket_info.bucket << "/" << key << dendl;
        }

        const RGWAccessControlList& acl = policy.get_acl();

        permissions.insert(policy.get_owner().get_id().to_str());
        for (auto acliter : acl.get_grant_map()) {
          const ACLGrant& grant = acliter.second;
          if (grant.get_type().get_type() == ACL_TYPE_CANON_USER &&
              ((uint32_t)grant.get_permission().get_permissions() & RGW_PERM_READ) != 0) {
            rgw_user user;
            if (grant.get_id(user)) {
              permissions.insert(user.to_str());
            }
          }
        }
      } else {
        if (name != "pg_ver" &&
            name != "source_zone" &&
            name != "idtag") {
          out_attrs[name] = string(val.c_str(), (val.length() > 0 ? val.length() - 1 : 0));
        }
      }
    }
    ::encode_json("bucket", bucket_info.bucket.name, f);
    ::encode_json("name", key.name, f);
    ::encode_json("instance", key.instance, f);
    ::encode_json("owner", policy.get_owner(), f);
    ::encode_json("permissions", permissions, f);
    f->open_object_section("meta");
    ::encode_json("size", size, f);

    string mtime_str;
    rgw_to_iso8601(mtime, &mtime_str);
    ::encode_json("mtime", mtime_str, f);
    for (auto i : out_attrs) {
      ::encode_json(i.first.c_str(), i.second, f);
    }
    map<string, string> custom_str;
    map<string, string> custom_int;
    map<string, string> custom_date;

    for (auto i : custom_meta) {
      auto config = bucket_info.mdsearch_config.find(i.first);
      if (config == bucket_info.mdsearch_config.end()) {
        if (!es_conf->explicit_custom_meta) {
          /* default custom meta is of type string */
          custom_str[i.first] = i.second;
        } else {
          ldout(cct, 20) << "custom meta entry key=" << i.first << " not found in bucket mdsearch config: " << bucket_info.mdsearch_config << dendl;
        }
        continue;
      }
      switch (config->second) {
        case ESEntityTypeMap::ES_ENTITY_DATE:
          custom_date[i.first] = i.second;
          break;
        case ESEntityTypeMap::ES_ENTITY_INT:
          custom_int[i.first] = i.second;
          break;
        default:
          custom_str[i.first] = i.second;
      }
    }

    if (!custom_str.empty()) {
      f->open_array_section("custom-string");
      for (auto i : custom_str) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    if (!custom_int.empty()) {
      f->open_array_section("custom-int");
      for (auto i : custom_int) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    if (!custom_date.empty()) {
      f->open_array_section("custom-date");
      for (auto i : custom_date) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    f->close_section();
  }
};

class RGWElasticInitConfigCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  ElasticConfigRef conf;
public:
  RGWElasticInitConfigCBCR(RGWDataSyncEnv *_sync_env,
                          ElasticConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                                    sync_env(_sync_env),
                                                    conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init elasticsearch config zone=" << sync_env->source_zone << dendl;
      yield {
        string path = es_get_index_path(sync_env->store->get_realm());

        es_index_mappings doc;

        call(new RGWPutRESTResourceCR<es_index_mappings, int>(sync_env->cct, conf->conn,
                                                              sync_env->http_manager,
                                                              path, nullptr /* params */,
                                                              doc, nullptr /* result */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  ElasticConfigRef conf;
public:
  RGWElasticHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          ElasticConfigRef _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      yield {
        string path = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);
        es_obj_metadata doc(sync_env->cct, conf, bucket_info, key, mtime, size, attrs);

        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf->conn,
                                                            sync_env->http_manager,
                                                            path, nullptr /* params */,
                                                            doc, nullptr /* result */));

      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWElasticHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  ElasticConfigRef conf;
public:
  RGWElasticHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        ElasticConfigRef _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf) {
  }

  ~RGWElasticHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWElasticHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWElasticRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  ElasticConfigRef conf;
public:
  RGWElasticRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          ElasticConfigRef _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf->conn,
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticDataSyncModule : public RGWDataSyncModule {
  ElasticConfigRef conf;
public:
  RGWElasticDataSyncModule(CephContext *cct, const map<string, string, ltstr_nocase>& config) : conf(std::make_shared<ElasticConfig>()) {
    ElasticConfigRef default_module(std::make_shared<ElasticConfig>());
    string elastic_endpoint = rgw_conf_get(config, "endpoint", "");
    conf->id = string("elastic:") + elastic_endpoint;
    conf->conn = new RGWRESTConn(cct, nullptr, conf->id, { elastic_endpoint });
    conf->explicit_custom_meta = rgw_conf_get_bool(config, "explicit_custom_meta", true);
    conf->index_buckets.init(rgw_conf_get(config, "index_buckets_list", ""), true); /* approve all buckets by default */
    conf->allow_owners.init(rgw_conf_get(config, "approved_owners_list", ""), true); /* approve all bucket owners by default */
  }
  ~RGWElasticDataSyncModule() override {
    delete conf->conn;
  }

  RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": init" << dendl;
    return new RGWElasticInitConfigCBCR(sync_env, conf);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
    return new RGWElasticHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
    return new RGWElasticRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    ldout(sync_env->cct, 10) << conf->id << ": skipping operation (not handled)" << dendl;
    return NULL;
  }
  RGWRESTConn *get_rest_conn() {
    return conf->conn;
  }
};

RGWElasticSyncModuleInstance::RGWElasticSyncModuleInstance(CephContext *cct, const map<string, string, ltstr_nocase>& config)
{
  data_handler = std::unique_ptr<RGWElasticDataSyncModule>(new RGWElasticDataSyncModule(cct, config));
}

RGWDataSyncModule *RGWElasticSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTConn *RGWElasticSyncModuleInstance::get_rest_conn()
{
  return data_handler->get_rest_conn();
}

string RGWElasticSyncModuleInstance::get_index_path(const RGWRealm& realm) {
  return es_get_index_path(realm);
}

RGWRESTMgr *RGWElasticSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  return new RGWRESTMgr_MDSearch_S3(this);
}

int RGWElasticSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWElasticSyncModuleInstance(cct, config));
  return 0;
}

