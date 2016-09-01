#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_s3export.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"

#define dout_subsys ceph_subsys_rgw


class RGWStreamRemoteObjCR : public RGWCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  rgw_obj obj;
  param_vec_t params;

  RGWRESTStreamRWRequest *stream_req{nullptr};

  class RGWStreamRemoteObjCR_CB : public RGWGetDataCB {
    RGWStreamRemoteObjCR *cr;
  public:
    RGWStreamRemoteObjCR_CB(RGWStreamRemoteObjCR *_cr) : cr(_cr) {}
    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
      cr->append_in_data(bl);
      // cr->set_sleeping(false);
dout(0) << __FILE__ << ":" << __LINE__ << ": bl_ofs=" << bl_ofs << " bl_len=" << bl_len << dendl;
      return bl_len;
    }

  } cb;

  bufferlist in_data;

  Mutex lock;


public:
  RGWStreamRemoteObjCR(CephContext *_cct, RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager,
                       const rgw_obj& _obj,
                       rgw_http_param_pair *params)
    : RGWCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      obj(_obj), params(make_param_list(params)), cb(this), lock("RGWStreamRemoteObjCR")
  {}

  int operate() {
    reenter(this) {
      yield {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
        int ret = conn->get_obj(string() /* user_id */, nullptr, obj,
                                nullptr, nullptr,
                                0, 0,
                                true /* prepend_meta */, true /* GET */, false /* rgwx-stat */,
                                &cb, &stream_req,
                                http_manager);
        if (ret < 0) {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
          return set_cr_error(ret);
        }
        // set_sleeping(true);
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
        return io_block(0);
      }

      yield {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
        string etag;
        map<string, string> attrs;
        int ret = conn->complete_request(stream_req, etag, nullptr, nullptr, attrs);
        if (ret < 0) {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
          return set_cr_error(ret);
        }
      }

dout(0) << __FILE__ << ":" << __LINE__ << dendl;
      return set_cr_done();
    }

    return 0;
  }

  void wakeup() {
    set_sleeping(false);
  }

  void append_in_data(bufferlist& bl) {
    Mutex::Locker l(lock);
    in_data.claim_append(bl);
  }
};


struct S3ExportConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

static string es_get_obj_path(const RGWRealm& realm, const RGWBucketInfo& bucket_info, const rgw_obj_key& key)
{
  string path = "/rgw-" + realm.get_name() + "/object/" + bucket_info.bucket.bucket_id + ":" + key.name + ":" + key.instance;
  return path;
}


class RGWS3ExportHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  const S3ExportConfig& conf;
public:
  RGWS3ExportHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          const S3ExportConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      yield {
        string resource = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);
#if 0
        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf.conn,
                                                            sync_env->http_manager,
                                                            path, nullptr /* params */,
                                                            doc, nullptr /* result */));
#endif
        rgw_obj obj(bucket_info.bucket, key);
        call(new RGWStreamRemoteObjCR(sync_env->cct, sync_env->conn,
                       sync_env->http_manager, obj, nullptr));

      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWS3ExportHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  const S3ExportConfig& conf;
public:
  RGWS3ExportHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        const S3ExportConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf) {
  }

  ~RGWS3ExportHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWS3ExportHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWS3ExportRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  const S3ExportConfig& conf;
public:
  RGWS3ExportRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          const S3ExportConfig& _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf.conn,
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

class RGWS3ExportDataSyncModule : public RGWDataSyncModule {
  S3ExportConfig conf;
public:
  RGWS3ExportDataSyncModule(CephContext *cct, const string& elastic_endpoint) {
    conf.id = string("elastic:") + elastic_endpoint;
    conf.conn = new RGWRESTConn(cct, nullptr, conf.id, { elastic_endpoint });
  }
  ~RGWS3ExportDataSyncModule() {
    delete conf.conn;
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWS3ExportHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 0) << conf.id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWS3ExportRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWS3ExportSyncModuleInstance : public RGWSyncModuleInstance {
  RGWS3ExportDataSyncModule data_handler;
public:
  RGWS3ExportSyncModuleInstance(CephContext *cct, const string& endpoint) : data_handler(cct, endpoint) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWS3ExportSyncModule::create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWS3ExportSyncModuleInstance(cct, endpoint));
  return 0;
}

