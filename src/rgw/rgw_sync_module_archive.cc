#include "rgw_sync_module_archive.h"
#include "rgw_metadata.h"
#include "rgw_bucket.h"
#include "rgw_cr_rados.h"

#define dout_subsys ceph_subsys_rgw

static void get_md5_digest(const RGWBucketEntryPoint *be, string& md5_digest) {

   char md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
   unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
   bufferlist bl;

   JSONFormatter f(false);
   be->dump(&f);
   f.flush(bl);

   MD5 hash;
   hash.Update((const unsigned char *)bl.c_str(), bl.length());
   hash.Final(m);

   buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, md5);

   md5_digest = md5;
}

class RGWArchiveBucketMetadataHandler : public RGWBucketMetadataHandler {
public:
  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, RGWMetadataHandler::sync_type_t type) override {
    // archive existing bucket if needed
    size_t found = entry.find("-deleted-");
    if(found != string::npos) {
      RGWObjVersionTracker ot;
      int r = remove(store, entry, ot);
      if (r < 0) {
        ldout(store->ctx(), 20) << __func__ << "(): remove() returned r=" << r << dendl;
        /* ignore error */
      }
    }
    return RGWBucketMetadataHandler::put(store, entry, objv_tracker, mtime, obj, type);
  }

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    string metadata_key = "bucket:" + entry;

    ldout(store->ctx(), 0) << "SKIP: bucket removal is not allowed on archive zone: " << metadata_key << " ... proceeding to rename" << dendl;

    RGWBucketEntryPoint be;
    RGWObjectCtx obj_ctx(store);

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);

    real_time mtime;

    int ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, be, &objv_tracker, &mtime, NULL);
    if (ret < 0) {
        return ret;
    }

    string md5_digest;
    get_md5_digest(&be, md5_digest);

    string archive_zone_suffix = "-deleted-" + md5_digest;
    be.bucket.name = be.bucket.name + archive_zone_suffix;

    RGWBucketEntryMetadataObject *be_mdo = new RGWBucketEntryMetadataObject(be, objv_tracker.read_version, mtime);

    JSONFormatter f(false);
    f.open_object_section("metadata_info");
    encode_json("key", metadata_key + archive_zone_suffix, &f);
    encode_json("ver", be_mdo->get_version(), &f);
    mtime = be_mdo->get_mtime();
    if (!real_clock::is_zero(mtime)) {
       utime_t ut(mtime);
       encode_json("mtime", ut, &f);
    }
    encode_json("data", *be_mdo, &f);
    f.close_section();

    delete be_mdo;

    ret = rgw_unlink_bucket(store, be.owner, tenant_name, bucket_name, false);
    if (ret < 0) {
        lderr(store->ctx()) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = rgw_bucket_delete_bucket_obj(store, tenant_name, bucket_name, objv_tracker);
    if (ret < 0) {
        lderr(store->ctx()) << "could not delete bucket=" << entry << dendl;
    }

    string new_entry, new_bucket_name;
    new_entry = entry + archive_zone_suffix;
    parse_bucket(new_entry, &tenant_name, &new_bucket_name);

    bufferlist bl;
    f.flush(bl);

    JSONParser parser;
    if (!parser.parse(bl.c_str(), bl.length())) {
        return -EINVAL;
    }

    JSONObj *jo = parser.find_obj("data");
    if (!jo) {
        return -EINVAL;
    }

    try {
        decode_json_obj(be, jo);
    } catch (JSONDecoder::err& e) {
        return -EINVAL;
    }

    RGWBucketEntryPoint ep;
    ep.linked = be.linked;
    ep.owner = be.owner;
    ep.bucket = be.bucket;

    RGWObjVersionTracker ot;
    ot.generate_new_write_ver(store->ctx());

    map<string, bufferlist> attrs;

    ret = store->put_bucket_entrypoint_info(tenant_name, new_bucket_name, ep, false, ot, mtime, &attrs);
    if (ret < 0) {
        return ret;
    }

    ret = rgw_link_bucket(store, be.owner, be.bucket, be.creation_time, false);
    if (ret < 0) {
        return ret;
    }

    // .bucket.meta.my-bucket-1:c0f7ef8c-2309-4ebb-a1d0-1b0a61dc5a78.4226.1
    string meta_name = bucket_name + ":" + be.bucket.marker;

    map<string, bufferlist> attrs_m;
    RGWBucketInfo bi_m;

    ret = store->get_bucket_instance_info(obj_ctx, meta_name, bi_m, NULL, &attrs_m);
    if (ret < 0) {
        return ret;
    }

    string new_meta_name = RGW_BUCKET_INSTANCE_MD_PREFIX + new_bucket_name + ":" + be.bucket.marker;

    bi_m.bucket.name = new_bucket_name;

    bufferlist bl_m;
    using ceph::encode;
    encode(bi_m, bl_m);

    ret = rgw_put_system_obj(store, store->get_zone_params().domain_root, new_meta_name, bl_m, false, NULL, real_time(), NULL);
    if (ret < 0) {
        return ret;
    }

    ret = rgw_delete_system_obj(store, store->get_zone_params().domain_root, RGW_BUCKET_INSTANCE_MD_PREFIX + meta_name, NULL);

    /* idempotent */
    return 0;
  }

};

class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
public:
  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
     ldout(store->ctx(), 0) << "SKIP: bucket instance removal is not allowed on archive zone: " << entry << dendl;
     return 0;
   }
};

class RGWArchiveDataSyncModule : public RGWDefaultDataSyncModule {
public:
  RGWArchiveDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
};

class RGWArchiveSyncModuleInstance : public RGWDefaultSyncModuleInstance {
  RGWArchiveDataSyncModule data_handler;
public:
  RGWArchiveSyncModuleInstance() {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }

  bool alloc_metadata_handler(const string& type, RGWMetadataHandler **handler) {
    if (type == "bucket") {
      *handler = new RGWArchiveBucketMetadataHandler();
      return true;
    }
    if (type == "bucket.instance") {
      *handler = new RGWArchiveBucketInstanceMetadataHandler();
      return true;
    }
    return false;
  }
};

int RGWArchiveSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
{
  instance->reset(new RGWArchiveSyncModuleInstance());
  return 0;
}

RGWCoroutine *RGWArchiveDataSyncModule::sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sync_env->cct, 0) << "SYNC_ARCHIVE: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
  if (!bucket_info.versioned() ||
     (bucket_info.flags & BUCKET_VERSIONS_SUSPENDED)) {
      ldout(sync_env->cct, 0) << "SYNC_ARCHIVE: sync_object: enabling object versioning for archive bucket" << dendl;
      bucket_info.flags = (bucket_info.flags & ~BUCKET_VERSIONS_SUSPENDED) | BUCKET_VERSIONED;
      int op_ret = sync_env->store->put_bucket_instance_info(bucket_info, false, real_time(), NULL);
      if (op_ret < 0) {
         ldout(sync_env->cct, 0) << "SYNC_ARCHIVE: sync_object: error versioning archive bucket" << dendl;
         return NULL;
      }
  }
  return new RGWFetchRemoteObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone, bucket_info,
                                 key, versioned_epoch,
                                 true, zones_trace);
}

RGWCoroutine *RGWArchiveDataSyncModule::remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key,
                                                     real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sync_env->cct, 0) << "SYNC_ARCHIVE: remove_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
  return NULL;
}

RGWCoroutine *RGWArchiveDataSyncModule::create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                                            rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sync_env->cct, 0) << "SYNC_ARCHIVE: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
	                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
  return new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone,
                            bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

