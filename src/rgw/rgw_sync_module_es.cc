#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_es.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"

#define dout_subsys ceph_subsys_rgw

struct ElasticConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

static string es_get_obj_path(const RGWBucketInfo& bucket_info, const rgw_obj_key& key)
{
  string path = "/rgw/object/" + bucket_info.bucket.bucket_id + ":" + key.name + ":" + key.instance;
  return path;
}

struct es_obj_metadata {
  CephContext *cct;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  uint64_t size;
  map<string, bufferlist> attrs;

  es_obj_metadata(CephContext *_cct, const RGWBucketInfo& _bucket_info, const rgw_obj_key& _key,
                  uint64_t _size, map<string, bufferlist>& _attrs) : cct(_cct), bucket_info(_bucket_info), key(_key),
                                                                     size(_size), attrs(std::move(_attrs)) {}

  void dump(Formatter *f) const {
    map<string, string> out_attrs;
    RGWAccessControlPolicy policy;
    set<string> permissions;
    for (auto i : attrs) {
      const string& attr_name = i.first;
      string name;
      bufferlist& val = i.second;

      if (attr_name.compare(0, sizeof(RGW_ATTR_PREFIX) - 1, RGW_ATTR_PREFIX) != 0) {
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
    for (auto i : out_attrs) {
      ::encode_json(i.first.c_str(), i.second, f);
    }
    f->close_section();
  }

};

class RGWElasticHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          const ElasticConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      yield {
        string path = es_get_obj_path(bucket_info, key);
        es_obj_metadata doc(sync_env->cct, bucket_info, key, size, attrs);

        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf.conn,
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
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        const ElasticConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf) {
  }

  ~RGWElasticHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWElasticHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWElasticDataSyncModule : public RGWDataSyncModule {
  ElasticConfig conf;
public:
  RGWElasticDataSyncModule(CephContext *cct, const string& elastic_endpoint) {
    conf.id = string("elastic:") + elastic_endpoint;
    conf.conn = new RGWRESTConn(cct, nullptr, conf.id, { elastic_endpoint });
  }
  ~RGWElasticDataSyncModule() {
    delete conf.conn;
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWElasticHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWElasticSyncModuleInstance : public RGWSyncModuleInstance {
  RGWElasticDataSyncModule data_handler;
public:
  RGWElasticSyncModuleInstance(CephContext *cct, const string& endpoint) : data_handler(cct, endpoint) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWElasticSyncModule::create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWElasticSyncModuleInstance(cct, endpoint));
  return 0;
}

