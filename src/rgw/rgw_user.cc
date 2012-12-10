#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
#include "rgw_rados.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"
#include "rgw_string.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
void rgw_get_anon_user(RGWUserInfo& info)
{
  info.user_id = RGW_USER_ANON_ID;
  info.display_name.clear();
  info.access_keys.clear();
}

bool rgw_user_is_authenticated(RGWUserInfo& info)
{
  return (info.user_id != RGW_USER_ANON_ID);
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWRados *store, RGWUserInfo& info, bool exclusive)
{
  bufferlist bl;
  info.encode(bl);
  string md5;
  int ret;
  map<string,bufferlist> attrs;

  map<string, RGWAccessKey>::iterator iter;
  for (iter = info.swift_keys.begin(); iter != info.swift_keys.end(); ++iter) {
    RGWAccessKey& k = iter->second;
    /* check if swift mapping exists */
    RGWUserInfo inf;
    int r = rgw_get_user_info_by_swift(store, k.id, inf);
    if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
      ldout(store->ctx(), 0) << "WARNING: can't store user info, swift id already mapped to another user" << dendl;
      return -EEXIST;
    }
  }

  if (info.access_keys.size()) {
    /* check if access keys already exist */
    RGWUserInfo inf;
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      int r = rgw_get_user_info_by_access_key(store, k.id, inf);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(store->ctx(), 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }
  }

  bufferlist uid_bl;
  RGWUID ui;
  ui.user_id = info.user_id;
  ::encode(ui, uid_bl);
  ::encode(info, uid_bl);

  ret = rgw_put_system_obj(store, store->params.user_uid_pool, info.user_id, uid_bl.c_str(), uid_bl.length(), exclusive);
  if (ret < 0)
    return ret;

  if (info.user_email.size()) {
    ret = rgw_put_system_obj(store, store->params.user_email_pool, info.user_email, uid_bl.c_str(), uid_bl.length(), exclusive);
    if (ret < 0)
      return ret;
  }

  if (info.access_keys.size()) {
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      ret = rgw_put_system_obj(store, store->params.user_keys_pool, k.id, uid_bl.c_str(), uid_bl.length(), exclusive);
      if (ret < 0)
        return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter;
  for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    ret = rgw_put_system_obj(store, store->params.user_swift_pool, k.id, uid_bl.c_str(), uid_bl.length(), exclusive);
    if (ret < 0)
      return ret;
  }

  return ret;
}

int rgw_get_user_info_from_index(RGWRados *store, string& key, rgw_bucket& bucket, RGWUserInfo& info)
{
  bufferlist bl;
  RGWUID uid;

  int ret = rgw_get_obj(store, NULL, bucket, key, bl);
  if (ret < 0)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(uid, iter);
    if (!iter.end())
      info.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_uid(RGWRados *store, string& uid, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, uid, store->params.user_uid_pool, info);
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_email(RGWRados *store, string& email, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, email, store->params.user_email_pool, info);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados *store, string& swift_name, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, swift_name, store->params.user_swift_pool, info);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados *store, string& access_key, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, access_key, store->params.user_keys_pool, info);
}

static void get_buckets_obj(string& user_id, string& buckets_obj_id)
{
  buckets_obj_id = user_id;
  buckets_obj_id += RGW_BUCKETS_OBJ_PREFIX;
}

static int rgw_read_buckets_from_attr(RGWRados *store, string& user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  rgw_obj obj(store->params.user_uid_pool, user_id);
  int ret = store->get_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);
  if (ret)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    buckets.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode buckets info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets, bool need_stats)
{
  int ret;
  buckets.clear();
  if (store->supports_omap()) {
    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);
    bufferlist bl;
    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    bufferlist header;
    map<string,bufferlist> m;

    ret = store->omap_get_all(obj, header, m);
    if (ret == -ENOENT)
      ret = 0;

    if (ret < 0)
      return ret;

    for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
      bufferlist::iterator iter = q->second.begin();
      RGWBucketEnt bucket;
      ::decode(bucket, iter);
      buckets.add(bucket);
    }
  } else {
    ret = rgw_read_buckets_from_attr(store, user_id, buckets);
    switch (ret) {
    case 0:
      break;
    case -ENODATA:
      ret = 0;
      return 0;
    default:
      return ret;
    }
  }

  list<string> buckets_list;

  if (need_stats) {
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    int r = store->update_containers_stats(m);
    if (r < 0)
      ldout(store->ctx(), 0) << "ERROR: could not get stats for buckets" << dendl;

  }
  return 0;
}

/**
 * Store the set of buckets associated with a user on a n xattr
 * not used with all backends
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);

  rgw_obj obj(store->params.user_uid_pool, user_id);

  int ret = store->set_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);

  return ret;
}

int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;
  string& bucket_name = bucket.name;

  if (store->supports_omap()) {
    bufferlist bl;

    RGWBucketEnt new_bucket;
    new_bucket.bucket = bucket;
    new_bucket.size = 0;
    time(&new_bucket.mtime);
    ::encode(new_bucket, bl);

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    ret = store->omap_set(obj, bucket_name, bl);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error adding bucket to directory: "
          << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);
    RGWBucketEnt new_bucket;

    switch (ret) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.bucket = bucket;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
      break;
    default:
      ldout(store->ctx(), 10) << "rgw_write_buckets_attr returned " << ret << dendl;
      break;
    }
  }

  return ret;
}

int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;

  if (store->supports_omap()) {
    bufferlist bl;

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    ret = store->omap_del(obj, bucket.name);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error removing bucket from directory: "
          << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);

    if (ret == 0 || ret == -ENOENT) {
      buckets.remove(bucket.name);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
    }
  }

  return ret;
}

int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key)
{
  rgw_obj obj(store->params.user_keys_pool, access_key.id);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_uid_index(RGWRados *store, string& uid)
{
  rgw_obj obj(store->params.user_uid_pool, uid);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_email_index(RGWRados *store, string& email)
{
  rgw_obj obj(store->params.user_email_pool, email);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_swift_name_index(RGWRados *store, string& swift_name)
{
  rgw_obj obj(store->params.user_swift_pool, swift_name);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWRados *store, RGWUserInfo& info) {
  RGWUserBuckets user_buckets;
  int ret = rgw_read_user_buckets(store, info.user_id, user_buckets, false);
  if (ret < 0)
    return ret;

  map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
  vector<rgw_bucket> buckets_vec;
  for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
      i != buckets.end();
      ++i) {
    RGWBucketEnt& bucket = i->second;
    buckets_vec.push_back(bucket.bucket);
  }
  map<string, RGWAccessKey>::iterator kiter = info.access_keys.begin();
  for (; kiter != info.access_keys.end(); ++kiter) {
    ldout(store->ctx(), 10) << "removing key index: " << kiter->first << dendl;
    ret = rgw_remove_key_index(store, kiter->second);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not remove " << kiter->first << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter = info.swift_keys.begin();
  for (; siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    ldout(store->ctx(), 10) << "removing swift subuser index: " << k.id << dendl;
    /* check if swift mapping exists */
    ret = rgw_remove_swift_name_index(store, k.id);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not remove " << k.id << " (swift name object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  rgw_obj email_obj(store->params.user_email_pool, info.user_email);
  ldout(store->ctx(), 10) << "removing email index: " << info.user_email << dendl;
  ret = store->delete_obj(NULL, email_obj);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << email_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string buckets_obj_id;
  get_buckets_obj(info.user_id, buckets_obj_id);
  rgw_obj uid_bucks(store->params.user_uid_pool, buckets_obj_id);
  ldout(store->ctx(), 10) << "removing user buckets index" << dendl;
  ret = store->delete_obj(NULL, uid_bucks);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }
  
  rgw_obj uid_obj(store->params.user_uid_pool, info.user_id);
  ldout(store->ctx(), 10) << "removing user index: " << info.user_id << dendl;
  ret = store->delete_obj(NULL, uid_obj);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}

/* new functionality */

static bool char_is_unreserved_url(char c)
{
  if (isalnum(c))
    return true;

  switch (c) {
  case '-':
  case '.':
  case '_':
  case '~':
    return true;
  default:
    return false;
  }
}

// define as static when changes complete
bool validate_access_key(string& key)
{
  const char *p = key.c_str();
  while (*p) {
    if (!char_is_unreserved_url(*p))
      return false;
    p++;
  }
  return true;
}

// define as static when changes complete
int remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object)
{
  int ret = -EINVAL;
  RGWRadosCtx rctx(store);

  rgw_obj obj(bucket,object);

  ret = store->delete_obj((void *)&rctx, obj);
  
  return ret;
}

// define as static when changes complete
int remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children)
{
  int ret;
  map<RGWObjCategory, RGWBucketStats> stats;
  std::vector<RGWObjEnt> objs;
  std::string prefix, delim, marker, ns;
  map<string, bool> common_prefixes;
  rgw_obj obj;
  RGWBucketInfo info;
  bufferlist bl;

  ret = store->get_bucket_stats(bucket, stats);
  if (ret < 0)
    return ret;

  obj.bucket = bucket;
  int max = 1000;

  ret = rgw_get_obj(store, NULL, store->params.domain_root,\
           bucket.name, bl, NULL);

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(info, iter);
  } catch (buffer::error& err) {
    //cerr << "ERROR: could not decode buffer info, caught buffer::error" << std::endl;
    return -EIO;
  }

  if (delete_children) {
    ret = store->list_objects(bucket, max, prefix, delim, marker,\
            objs, common_prefixes,\
            false, ns, (bool *)false, NULL);

    if (ret < 0)
      return ret;

    while (objs.size() > 0) {
      std::vector<RGWObjEnt>::iterator it = objs.begin();
      for (it = objs.begin(); it != objs.end(); it++) {
        ret = remove_object(store, bucket, (*it).name);
        if (ret < 0)
          return ret;
      }
      objs.clear();

      ret = store->list_objects(bucket, max, prefix, delim, marker, objs, common_prefixes,
                                false, ns, (bool *)false, NULL);
      if (ret < 0)
        return ret;
    }
  }

  ret = store->delete_bucket(bucket);
  if (ret < 0) {
    //cerr << "ERROR: could not remove bucket " << bucket.name << std::endl;

    return ret;
  }

  ret = rgw_remove_user_bucket_info(store, info.owner, bucket);
  if (ret < 0) {
    //cerr << "ERROR: unable to remove user bucket information" << std::endl;
  }

  return ret;
}

static bool remove_old_indexes(RGWRados *store,\
         RGWUserInfo& old_info, RGWUserInfo& new_info, std::string &err_msg)
{
  int ret;
  bool success = true;

  if (!old_info.user_id.empty() && old_info.user_id.compare(new_info.user_id) != 0) {
    ret = rgw_remove_uid_index(store, old_info.user_id);
    if (ret < 0 && ret != -ENOENT) {
      err_msg =  "ERROR: could not remove index for uid " + old_info.user_id;
      success = false;
    }
  }

  if (!old_info.user_email.empty() &&
      old_info.user_email.compare(new_info.user_email) != 0) {
    ret = rgw_remove_email_index(store, old_info.user_email);
  if (ret < 0 && ret != -ENOENT) {
      err_msg = "ERROR: could not remove index for email " + old_info.user_email;
      success = false;
    }
  }

  map<string, RGWAccessKey>::iterator old_iter;
  for (old_iter = old_info.swift_keys.begin(); old_iter != old_info.swift_keys.end(); ++old_iter) {
    RGWAccessKey& swift_key = old_iter->second;
    map<string, RGWAccessKey>::iterator new_iter = new_info.swift_keys.find(swift_key.id);
    if (new_iter == new_info.swift_keys.end()) {
      ret = rgw_remove_swift_name_index(store, swift_key.id);
      if (ret < 0 && ret != -ENOENT) {
        err_msg =  "ERROR: could not remove index for swift_name " + swift_key.id;
        success = false;
      }
    }
  }



  return success;
}

static bool get_key_type(std::string requested_type,\
         int &dest, string &err_msg)
{
  if (strcasecmp(requested_type.c_str(), "swift") == 0) {
    dest = KEY_TYPE_SWIFT;
  } else if (strcasecmp(requested_type.c_str(), "s3") == 0) {
    dest = KEY_TYPE_S3;
  } else {
    err_msg = "bad key type";
    return false;
  }

  return true;
}


bool rgw_build_user_request_from_map(map<std::string,\
         std::string> request_params, RGWUserAdminRequest &_req,\
         std::string &err_msg)
{
  int ret = 0;

  map<std::string, std::string>::iterator it;
  //std::string subprocess_msg;
  RGWUserInfo duplicate_info;

  RGWUserAdminRequest req;

  /* do access key related building */
  
  // see if a key type was specified
  it = request_params.find("key_type");
  if (it != request_params.end()) {
    req.type_specified = get_key_type(it->second, req.key_type, err_msg);
    if (!req.type_specified)
      return false;
  }

  // see if the access key or secret keys was specified
  it = request_params.find("access_key");
  if (it != request_params.end())
    req.id = it->first;
  
  it = request_params.find("secret_key");
  if (it != request_params.end())
    req.key = it->second;

  // get some other possible parameters
  it = request_params.find("gen_secret");
  if (it != request_params.end())
    req.gen_secret = str_to_bool(it->second.c_str(), 0);

  it = request_params.find("gen_access");
  if (it != request_params.end())
    req.gen_access = str_to_bool(it->second.c_str(), 0);

  /* do subuser related building */
  
  it = request_params.find("subuser");
  if (it != request_params.end())
    req.subuser = it->second;
 
  it = request_params.find("access");
  if (it != request_params.end()) {
    ret = stringtoul((it->second).c_str(), &req.perm_mask);
    if (ret < 0) {
      err_msg = "unable to parse perm mask";
      return false;
    }
    req.perm_specified = true;
  }

  /* user related building */

  it = request_params.find("user_id");
  if (it != request_params.end()) 
    req.user_id = it->second;

  it = request_params.find("user_email");
  if (it != request_params.end())
    req.user_email = it->second;
    
  it = request_params.find("display_name");
  if (it != request_params.end())
    req.display_name = it->second;
    
  // assume that if this was passed we are doing enable/disable op
  it = request_params.find("suspended");
  if (it != request_params.end()) {

    req.suspension_op = true;
    int suspended = str_to_bool(it->second.c_str(), -1);
    if (suspended < 0) {
      err_msg = "unable to parse suspension information";
      return false;
    }

    if (suspended)
      req.is_suspended = 1;

    else
      req.is_suspended = 0;
  }

  it = request_params.find("max_buckets");
  if (it != request_params.end()) {
    req.max_buckets_specified = true;
    
    ret = stringtoul(it->second, &req.max_buckets);
    if (ret < 0) {
      err_msg = "unable to parse max buckets information";
    return false;
    }
  }

  _req = req;

  return true;
}

RGWAccessKeyPool::RGWAccessKeyPool(RGWUser *_user)
{
  init(_user);
}

RGWAccessKeyPool::~RGWAccessKeyPool()
{

}

bool RGWAccessKeyPool::init(RGWUser *_user)
{
  if (_user->failure) {
    keys_allowed = false;
    return false;
  }

  store = _user->store;

  if (_user->user_id == RGW_USER_ANON_ID || !_user->populated) {
    keys_allowed = false;
    return false;
  }

  keys_allowed = true;

  user = _user;
  user_id = _user->user_id;
  swift_keys = &(_user->user_info.swift_keys);
  access_keys = &(_user->user_info.access_keys);

  return true;
}

bool RGWAccessKeyPool::check_existing_key(RGWUserAdminRequest &req)
{
  if (req.id.empty())
    return false;

  // if the key type was specified, great...
  if (req.key_type == KEY_TYPE_SWIFT) {
    req.existing_key = user->user_info.swift_keys.count(req.id);

    // see if the user made a mistake with the access key
    if (!req.user_id.empty() && !req.subuser.empty() && !req.existing_key) {
      string access_key = req.user_id;
      access_key.append(":");
      access_key.append(req.subuser);
      req.existing_key = user->user_info.swift_keys.count(access_key);

      if (req.existing_key)
        req.id = access_key;
    }
  }

  if (req.key_type == KEY_TYPE_S3) {
    req.existing_key = user->user_info.access_keys.count(req.id);
  }

  /*
   * ... if not since there is nothing preventing an S3 key from having a colon
   * we have to traverse both access key maps
   */

  // try the swift keys first
  if (!req.type_specified && !req.existing_key) {
    req.existing_key = user->user_info.swift_keys.count(req.id);

    // see if the user made a mistake with the access key
    if (!req.user_id.empty() && !req.subuser.empty() && !req.existing_key) {
      string access_key = req.user_id;
      access_key.append(":");
      access_key.append(req.subuser);
      req.existing_key = user->user_info.swift_keys.count(access_key);

      if (req.existing_key) {
        req.id = access_key;
      req.key_type = KEY_TYPE_SWIFT;
      }
    }
  }

  if (!req.type_specified && !req.existing_key) {
    req.existing_key = user->user_info.access_keys.count(req.id);

    if (req.existing_key)
      req.key_type = KEY_TYPE_S3;
  }

  if (req.existing_key)
    return true;

  return false;
}

bool RGWAccessKeyPool::check_request(RGWUserAdminRequest &req,\
     std::string &err_msg)
{
  bool found;
  string subprocess_msg;

  if (!user->populated) {
    found = user->init(req);
     if (!found) {
       err_msg = "unable to initialize user";
       return false;
     }
  }

  /* see if the access key or secret key was specified */
  if (req.id_specified && req.id.empty()) {
    err_msg = "empty access key";
    return false;
  }

  if (!req.id.empty())
    req.id_specified = true;

  if (req.key_specified && req.key.empty()) {
    err_msg = "empty secret key";
    return false;
  }

  if (!req.key.empty())
    req.key_specified = true;

  if (req.key_type == KEY_TYPE_SWIFT && !req.id_specified) {
    if (req.subuser.empty()) {
      err_msg = "swift key creation requires a subuser to be specified";
      return false;
    }
  }

  if (req.subuser_specified && req.subuser.empty()) {
    err_msg = "empty subuser";
    return false;
  }

  // check that the subuser exists
  if (req.subuser_specified && !user->subusers->exists(req.subuser)) {
    err_msg = "subuser does not exist";
    return false;
  }

  // one day it will be safe to force subusers to have swift keys
  //if (req.subuser_specified)
  //  req.key_type = KEY_TYPE_SWIFT;

  check_existing_key(req);

  // if a key type wasn't specified set it to s3
  if (req.key_type != KEY_TYPE_S3 && req.key_type != KEY_TYPE_SWIFT)
    req.key_type = KEY_TYPE_S3;

  if (!keys_allowed) {
    err_msg = "keys not allowed for this user";
    return false;
  }

  return true;
}

// Generate a new random key
bool RGWAccessKeyPool::generate_key(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string duplicate_check_id;
  std::string id;
  std::string key;
  std::string subuser;

  RGWAccessKey new_key;
  RGWUserInfo duplicate_check;

  int ret;
  bool duplicate = false;

  if (!keys_allowed)
    return false;

  if (req.id_specified)
    id = req.id;

  if (req.key_specified)
    key = req.key;

  // this isn't a modify key operation, return error if the key exists
  if (req.id_specified && req.key_type == KEY_TYPE_S3)
    duplicate = (rgw_get_user_info_by_access_key(store, req.id, duplicate_check) >= 0);

  else if (req.id_specified && req.key_type == KEY_TYPE_SWIFT)
    duplicate = (rgw_get_user_info_by_swift(store, id, duplicate_check) >= 0);

  if (duplicate) {
    err_msg = "cannot create duplicate access key: " + req.id;
    return false;
  }

  if (req.subuser_specified)
    new_key.subuser = req.subuser;


  // Generate the secret key
  if (!req.key_specified) {
    char secret_key_buf[SECRET_KEY_LEN + 1];

    ret = gen_rand_base64(g_ceph_context, secret_key_buf, sizeof(secret_key_buf));
    if (ret < 0) {
      err_msg = "unable to generate secret key";
      return false;
    }

    key = secret_key_buf;
  }

  // Generate the access key
  if (req.key_type == KEY_TYPE_S3 && !req.id_specified) {
    char public_id_buf[PUBLIC_ID_LEN + 1];

    do {
      int id_buf_size = sizeof(public_id_buf);
      ret = gen_rand_alphanumeric_upper(g_ceph_context,\
               public_id_buf, id_buf_size);

      if (ret < 0) {
        err_msg = "unable to generate access key";
        return false;
      }

      id = public_id_buf;
      if (!validate_access_key(id))
        continue;

    } while (!rgw_get_user_info_by_access_key(store, id, duplicate_check));
  }

  if (req.key_type == KEY_TYPE_SWIFT && !req.id_specified) {
    id = user_id;
    id.append(":");
    id.append(subuser);

    // check that the access key doesn't exist
    if (rgw_get_user_info_by_swift(store, id, duplicate_check)) {
      err_msg = "duplicate access key: " + id;
      return false;
    }
  }

  // finally create the new key
  new_key.id = id;
  new_key.key = key;

  if (req.key_type == KEY_TYPE_S3)
    user->user_info.access_keys[id] = new_key;
  else if (req.key_type == KEY_TYPE_SWIFT)
    user->user_info.swift_keys[id] = new_key;

  return true;
}

// modify an existing key
bool RGWAccessKeyPool::modify_key(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string id;
  std::string key;
  RGWAccessKey modify_key;

  pair<string, RGWAccessKey> key_pair;
  map<std::string, RGWAccessKey>::iterator kiter;

  if (!req.id_specified) {
    err_msg = "no access key specified";
    return false;
  }

  if (!req.existing_key) {
    err_msg = "key does not exist";
    return false;
  }

  key_pair.first = req.id;

  if (req.key_type == KEY_TYPE_SWIFT) {
    kiter = swift_keys->find(req.id);
    modify_key = kiter->second;
  }

  if (req.key_type == KEY_TYPE_S3) {
    kiter = access_keys->find(req.id);
    modify_key = kiter->second;
  }

  if (!req.key_specified) {
    char secret_key_buf[SECRET_KEY_LEN + 1];

    int ret;
    int key_buf_size = sizeof(secret_key_buf);
    ret  = gen_rand_base64(g_ceph_context, secret_key_buf, key_buf_size);
    if (ret < 0) {
      err_msg = "unable to generate secret key";
      return false;
    }

    key = secret_key_buf;
  } else {
    key = req.key;
  }

  if (key.empty()) {
      err_msg = "empty secret key";
      return false;
  }

  // update the access key with the new secret key
  modify_key.key = key;
  key_pair.second = modify_key;


  if (req.key_type == KEY_TYPE_S3)
    access_keys->insert(key_pair);

  else if (req.key_type == KEY_TYPE_SWIFT)
    swift_keys->insert(key_pair);

  return true;
}

bool RGWAccessKeyPool::execute_add(RGWUserAdminRequest &req,\
         std::string &err_msg, bool defer_save)
{
  bool created;
  bool updated = true;

  std::string subprocess_msg;

  int op = GENERATE_KEY;

  // set the op
  if (req.existing_key)
    op = MODIFY_KEY;

  switch (op) {
  case GENERATE_KEY:
    created = generate_key(req, subprocess_msg);
    break;
  case MODIFY_KEY:
    created = modify_key(req, subprocess_msg);
    break;
  }

  if (!created)
    return false;

  // store the updated info
  if (!defer_save) 
    updated = user->update(err_msg);

  if (!updated)
    return false;

  return true;
}

bool RGWAccessKeyPool::add(RGWUserAdminRequest &req, std::string &err_msg)
{
  bool created;
  bool checked;
  bool defer_save = false;
  std::string subprocess_msg;

  checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse request, " + subprocess_msg;
    return false;
  }

  created = execute_add(req, subprocess_msg, defer_save);
  if (!created) {
    err_msg = "unable to add access key, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWAccessKeyPool::execute_remove(RGWUserAdminRequest &req, std::string &err_msg, bool defer_save)
{
  bool updated =  true;
  map<std::string, RGWAccessKey>::iterator kiter;
  map<std::string, RGWAccessKey> *keys_map;


  if (!req.existing_key) {
    err_msg = "unable to find access key";
    return false;
  }

  // one day it will be safe to assume that subusers always have swift keys
  //if (req.subuser_specified)
  //  req.key_type = KEY_TYPE_SWIFT

  if (!req.existing_key) {
    err_msg = "unable to find access key";
    return false;
  }

  if (req.key_type == KEY_TYPE_S3){
    keys_map = access_keys;
    kiter = keys_map->find(req.id);
  }

  else if (req.key_type == KEY_TYPE_SWIFT) {
    keys_map = swift_keys;
    kiter = keys_map->find(req.id);
  }

  int ret = rgw_remove_key_index(store, kiter->second);
  if (ret < 0) {
    err_msg = "unable to remove key index";
    return false;
  }

  keys_map->erase(kiter);

  if (!defer_save)
    updated =  user->update(err_msg);

  if (!updated)
    return false;

  return true;
}

bool RGWAccessKeyPool::remove(RGWUserAdminRequest &req, std::string &err_msg)
{
  bool checked;
  bool removed;
  bool defer_save = false;

  std::string subprocess_msg;

  checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse request, " + subprocess_msg;
    return false;
  }

  removed = execute_remove(req, subprocess_msg, defer_save);
  if (!removed) {
    err_msg = "unable to remove subuser, " + subprocess_msg;
    return false;
  }

  return true;
}


RGWSubUserPool::RGWSubUserPool(RGWUser *_user)
{
  init(_user);
}

RGWSubUserPool::~RGWSubUserPool()
{

}

bool RGWSubUserPool::init(RGWUser *_user)
{
  if (!_user || _user->failure) {
    subusers_allowed = false;
    return false;
  }

  store = user->store;

  if (_user->user_id == RGW_USER_ANON_ID || !_user->populated) {
    subusers_allowed = false;
    return false;
  }

  subusers_allowed = true;
  user_id = _user->user_id;

  subuser_map = &(_user->user_info.subusers);

  return true;
}

bool RGWSubUserPool::exists(std::string subuser)
{
  if (!subuser_map)
    return false;

  if (subuser_map->count(subuser))
    return true;

  return false;
}

bool RGWSubUserPool::check_request(RGWUserAdminRequest &req,\
        std::string &err_msg)
{
  bool checked = true;
  bool found;
  string subprocess_msg;

  if (!user->populated) {
    found = user->init(req);
    if (!found) {
      err_msg = "unable to initialize user";
      return false;
    }
  }

  if (!req.subuser.empty()) {
    err_msg = "empty subuser name";
    return false;
  }

  req.subuser_specified = true;

  // check if the subuser exists
  req.existing_subuser = exists(req.subuser);

  // handle key requests
  bool key_op = (req.gen_secret || req.key_specified || req.purge_keys);
  if ( key_op && req.existing_subuser) {
    std::string access_key = user->user_id;
    access_key.append(":");
    access_key.append(req.subuser);

    // one day force subusers to have swift keys
    //req.key_type = KEY_TYPE_SWIFT;

    if (!req.id_specified)
      req.id = access_key;

    checked = user->keys->check_request(req, err_msg);
    if (checked)
      return false;
  }

  if (!subusers_allowed) {
    err_msg = "subusers not allowed for this user";
    return false;
  }

  return true;
}

bool RGWSubUserPool::execute_add(RGWUserAdminRequest &req,\
        std::string &err_msg, bool defer_save)
{
  bool defer_key_save = true;
  bool updated = true;
  std::string subprocess_msg;

  RGWSubUser subuser;

  // no duplicates
  if (req.existing_subuser) {
    err_msg = "subuser exists";
    return false;
  }

  if (req.key_specified || req.gen_secret) {
    bool keys_added = user->keys->execute_add(req, subprocess_msg, defer_key_save);
    if (!keys_added) {
      err_msg = "unable to create subuser key, " + subprocess_msg;
      return false;
    }
  }

  // create the subuser
  subuser.name = req.subuser;

  if (req.perm_specified)
    subuser.perm_mask = req.perm_mask;

  // insert the subuser into user info
  std::pair<std::string, RGWSubUser> subuser_pair;
  subuser_pair  = make_pair(req.subuser, subuser);
  subuser_map->insert(subuser_pair);

  // attempt to save the subuser
  if (!defer_save)
    updated = user->update(err_msg);

  if (!updated)
    return false;

  return true;
}

bool RGWSubUserPool::add(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  bool checked;
  bool created;
  bool defer_save = false;

  checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse request, " + subprocess_msg;
    return false;
  }

  created = execute_add(req, subprocess_msg, defer_save);
  if (!created) {
    err_msg = "unable to create subuser, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWSubUserPool::execute_remove(RGWUserAdminRequest &req,\
        std::string &err_msg, bool defer_save)
{
  bool updated = true;
  std::string subprocess_msg;

  map<std::string, RGWSubUser>::iterator siter;
  RGWUserAdminRequest key_request;

  if (!req.existing_subuser)
    err_msg = "subuser not found: " + req.subuser;


  if (req.purge_keys) {
    bool removed = user->keys->execute_remove(req, subprocess_msg, false);
    if (!removed) {
      err_msg = "unable to remove subuser keys, " + subprocess_msg;
      return false;
    }
  }

  //remove the subuser from the user info
  subuser_map->erase(siter);

  // attempt to save the subuser
  if (!defer_save)
    updated = user->update(err_msg);

  if (!updated)
    return false;

  return true;
}

bool RGWSubUserPool::remove(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  bool checked;
  bool removed;
  bool defer_save = false;

  checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse request, " + subprocess_msg;
    return false;
  }

  removed = execute_remove(req, subprocess_msg, defer_save);
  if (!removed) {
    err_msg = "unable to remove subuser, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWSubUserPool::execute_modify(RGWUserAdminRequest &req, std::string &err_msg, bool defer_save)
{
  bool updated = true;
  std::string subprocess_msg;
  std::map<std::string, RGWSubUser>::iterator siter;
  std::pair<std::string, RGWSubUser> subuser_pair;

  RGWSubUser subuser;

  if (!req.existing_subuser) {
    err_msg = "subuser does not exist";
    return false;
  }

  subuser_pair.first = req.subuser;

  siter = subuser_map->find(req.subuser);
  subuser = siter->second;

  bool success = user->keys->execute_add(req, subprocess_msg, true);
  if (!success) {
    err_msg = "unable to create subuser keys, " + subprocess_msg;
    return false;
  }

  if (req.perm_specified)
    subuser.perm_mask = req.perm_mask;

  subuser_pair.second = subuser;
  subuser_map->insert(subuser_pair);

  // attempt to save the subuser
  if (!defer_save)
    updated = user->update(err_msg);

  if (!updated)
    return false;

  return true;
}

bool RGWSubUserPool::modify(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  bool checked;
  bool modified;
  bool defer_save = false;

  RGWSubUser subuser;

  checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse request, " + subprocess_msg;
    return false;
  }

  modified = execute_modify(req, subprocess_msg, defer_save);
  if (!modified) {
    err_msg = "unable to modify subuser, " + subprocess_msg;
    return false;
  }

  return true;
}


RGWUserCapPool::RGWUserCapPool(RGWUser *_user)
{
  init(_user);
}

RGWUserCapPool::~RGWUserCapPool()
{

}

bool RGWUserCapPool::init(RGWUser *_user)
{
  if (!_user || _user->failure) {
    caps_allowed = false;
    return false;
  }

  if (_user->user_id == RGW_USER_ANON_ID || !_user->populated) {
    caps_allowed = false;
    return false;
  }

  caps_allowed = true;
  caps = &(_user->user_info.caps);

  return true;
}

bool RGWUserCapPool::add(RGWUserAdminRequest &req, std::string &err_msg)
{
  bool found;
  bool updated;
  std::string subprocess_msg;

  if (!user->populated) {
    found = user->init(req);
    if (!found) {
      err_msg = "unable to initialize user";
      return false;
    }
  }

  if (!caps_allowed) {
    err_msg = "caps not allowed for this user";
    return false;
  }

  if (req.caps.empty()) {
    err_msg = "empty user caps";
    return false;
  }

  int r = caps->add_from_string(req.caps);
  if (r < 0) {
    err_msg = "unable to add caps: " + req.caps;
    return false;
  }

  updated = user->update(err_msg);
  if (!updated)
    return false;

  return true;
}

bool RGWUserCapPool::remove(RGWUserAdminRequest &req, std::string &err_msg)
{
  bool found;
  bool updated;
  std::string subprocess_msg;

  if (!user->populated) {
    found = user->init(req);
    if (!found) {
      err_msg = "unable to initialize user";
      return false;
    }
  }

  if (!caps_allowed) {
    err_msg = "caps not allowed for this user";
    return false;
  }

  if (req.caps.empty()) {
    err_msg = "empty user caps";
    return false;
   }

  int r = caps->remove_from_string(req.caps);
  if (r < 0) {
    err_msg = "unable to remove caps: " + req.caps;
    return false;
  }

  updated = user->update(err_msg);
  if (!updated)
    return false;

  return true;
}

RGWUser::RGWUser(RGWRados *_store, pair<int, std::string> id)
{
  if (!_store) // should set failure here
    return;

  store = _store;

  populated = false;
  failure = false;

  /* API wrappers */
  keys = new RGWAccessKeyPool(this);
  caps = new RGWUserCapPool(this);
  subusers = new RGWSubUserPool(this);

  init(id);
}

RGWUser::RGWUser(RGWRados *_store, RGWUserAdminRequest &req)
{
  if (!_store) // should set failure here
    return;

  store = _store;

  populated = false;
  failure = false;

  /* API wrappers */
  keys = new RGWAccessKeyPool(this);
  caps = new RGWUserCapPool(this);
  subusers = new RGWSubUserPool(this);

  init(req);
}

RGWUser::~RGWUser()
{
  delete keys;
  delete caps;
  delete subusers;
}
 
RGWUser::RGWUser(RGWRados *_store)
{
  if (!_store) {
    set_failure();
    return;
  }
  populated = false;
  failure = false;

  store = _store;
  return;
}

RGWUser::RGWUser()
{
  populated = true;
  failure = false;

  rgw_get_anon_user(user_info);

  return;
}

bool RGWUser::init(pair<int, string> id)
{
  std::string id_value = id.second;
  bool found = false;

  switch (id.first) {
  case RGW_USER_ID:
    if (!id_value.empty())
      found = (rgw_get_user_info_by_uid(store, id_value, user_info) >= 0);
    break;
  case RGW_USER_EMAIL:
    if (!id_value.empty())
      found = (rgw_get_user_info_by_email(store, id_value, user_info) >= 0);
    break;
  case RGW_SWIFT_USERNAME:
    if (!id_value.empty())
      found = (rgw_get_user_info_by_swift(store, id_value, user_info) >= 0);
    break;
  case RGW_ACCESS_KEY:
    if (!id_value.empty())
      found = (rgw_get_user_info_by_access_key(store, id_value, user_info) >= 0);
    break;
  }

  if (!found) {
    failure = true;
    return false;
  }

  user_id = user_info.user_id;
  populated = true;
  old_info = user_info;

  // this may have been called by a helper object
  bool initialized = init_members();
  if (!initialized)
    return false;

  return true;
}

bool RGWUser::init(RGWUserAdminRequest &req)
{
  if (populated)
    return true;

  bool found = false;
  std::string swift_user; // should check if subuser is already fully-qualified
  if (!req.user_id.empty() && !req.subuser.empty())
    swift_user = req.user_id + ":" + req.subuser;

  if (!req.user_id.empty())
    found = (rgw_get_user_info_by_uid(store, req.user_id, user_info) >= 0);

  if (!req.user_email.empty() && !found)
    found = (rgw_get_user_info_by_email(store, req.user_email, user_info) >= 0);

  // check that this is correct
  if (!req.subuser.empty() && !found)
    found = (rgw_get_user_info_by_swift(store, swift_user, user_info) >= 0);

  if (!req.id.empty())
    found = (rgw_get_user_info_by_access_key(store, req.id, user_info) >= 0);

  if (!found) {
    failure = true;
    return false;
  }

  // set populated
  populated = true;

  user_id = user_info.user_id;
  old_info = user_info;

  // this may have been called by a helper object
  bool initialized = init_members();
  if (!initialized)
    return false;

  return true;
}

bool RGWUser::init_members()
{
  bool initialized = false;

  if (!keys || !subusers || !caps)
    return false;

  initialized = keys->init(this);
  if (!initialized)
    return false;

  initialized = subusers->init(this);
  if (!initialized)
    return false;

  initialized = caps->init(this);
  if (!initialized)
    return false;

  return true;
}

bool RGWUser::update(std::string &err_msg)
{
  int ret;
  std::string subprocess_msg;

  if (!store) {
    err_msg = "couldn't initialize storage";
    return false;
  }

  if (!populated) {
    err_msg = "user info not populated so not saved";
    return false;
  }

  ret = remove_old_indexes(store, old_info, user_info, subprocess_msg);
  if (ret < 0) {
    err_msg = "unable to remove old user info, " + subprocess_msg;
    return false;
  }

  ret = rgw_store_user_info(store, user_info, false);
  if (ret < 0) {
    err_msg = "unable to store user info";
    return false;
  }

  return true;
}

bool RGWUser::check_request(RGWUserAdminRequest &req, std::string &err_msg)
{
  int ret;
  std::string subprocess_msg;
  bool same_id;
  same_id = strcasecmp(req.user_id.c_str(), user_id.c_str()) == 0;


  RGWUserInfo duplicate_info;

  if (req.user_id.empty() && !populated) {
    err_msg = "no user id provided";
    return false;
  }

  // this check is somehow broken from above
  if (populated && !same_id) {
    err_msg = "user id mismatch, requested id: " + req.user_id\
            + " does not match: " + user_id;

    return false;
  }

  if (!req.user_email.empty())
    req.user_email_specified = true;

  if (!req.display_name.empty())
    req.display_name_specified = true;

  if (req.perm_mask > 0)
    req.perm_specified = true;

  if (req.is_suspended != user_info.suspended)
    req.suspension_op = true;

  if (req.max_buckets != RGW_DEFAULT_MAX_BUCKETS)
    req.max_buckets_specified = true;

  /*
   * keys->check_request() will have to be called separately
   * in the case of user creation
   */
  if (populated) {
    bool checked = keys->check_request(req, err_msg);
    if (!checked) {
      err_msg = "unable to parse key parameters";
      return false;
    }
  }

  // check if the user exists already
  ret = rgw_get_user_info_by_uid(store, req.user_id, duplicate_info);
  req.existing_user = (ret >= 0);

  return true;
}

bool RGWUser::execute_add(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  RGWUserAdminRequest key_request;
  bool updated = true;
  bool defer_save = true;

  // fail if the user exists already
  if (req.existing_user) {
    err_msg = "user: " + req.user_id + " exists";
    return false;
  }

  // fail if the user_info has already been populated
  if (populated) {
    err_msg = "cannot overwrite already populated user";
    return false;
  }

  // fail if the display name was not included
  if (!req.display_name_specified) {
    err_msg = "no display name specified";
    return false;
  }

  // set the user info
  user_info.user_id = req.user_id;
  user_id = req.user_id;
  user_info.display_name = req.display_name;

  if (req.user_email_specified)
    user_info.user_email = req.user_email;

  if (req.max_buckets_specified)
    user_info.max_buckets = req.max_buckets;

  populated = true;
  failure = false; // replace with function

  // update the helper objects
  bool initialized = init_members();
  if (!initialized) {
    err_msg = "unable to initialize user";
    return false;
  }


  // we could possibly just do a key add here
  // see if we need to add an access key
  bool checked = keys->check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to process key parameters, " + subprocess_msg;
    return false;
  }

  bool key_op = (req.id_specified || req.key_specified
      || req.gen_access || req.gen_secret);

  if (key_op) {
    bool success = keys->execute_add(req, subprocess_msg, defer_save);
    if (!success) {
      err_msg = "unable to create access key, " + subprocess_msg;
      return false;
    }
  }

  updated = update(err_msg);
  if (!updated)
    return false;

  return true;
}

bool RGWUser::add(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;

  bool checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse parameters, " + subprocess_msg;
    return false;
  }

  bool created = execute_add(req, subprocess_msg);
  if (!created) {
    err_msg = "unable to create user, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWUser::execute_remove(RGWUserAdminRequest &req, std::string &err_msg)
{
  RGWUserBuckets buckets;

  int ret;

  if (!req.existing_user) {
    err_msg = "user does not exist";
    return false;
  }

  // purge the data first
  if (req.purge_data) {
    ret = rgw_read_user_buckets(store, user_id, buckets, false);
    if (ret < 0) {
      err_msg = "unable to read user data";
      return false;
    }

    map<std::string, RGWBucketEnt>& m = buckets.get_buckets();
    
    if (m.size() > 0) {
      std::map<std::string, RGWBucketEnt>::iterator it;
      for (it = m.begin(); it != m.end(); it++) {
        ret = remove_bucket(store, ((*it).second).bucket, true);

         if (ret < 0) {
           err_msg = "unable to delete user data"; 
           return false;
         }
      }
    }
  }

  ret = rgw_delete_user(store, user_info);
  if (ret < 0) {
    err_msg = "unable to remove user from RADOS";
    return false;
  }

  return true;
}

bool RGWUser::remove(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  
  bool checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse parameters, " + subprocess_msg;
    return false;
  }

  bool removed = execute_remove(req, subprocess_msg);
  if (!removed) {
    err_msg = "unable to remove user, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWUser::execute_modify(RGWUserAdminRequest &req, string &err_msg)
{
  int email_check;
  bool same_email;

  bool defer_save = true;
  bool updated = true;
  int ret = 0;
  std::string subprocess_msg;

  RGWUserInfo duplicate_check;
 
  // ensure that the user info has been populated or is populate-able
  if (!req.existing_user && !populated) {
    err_msg = "user not found";
    return false;
  }

  // ensure that we can modify the user's attributes
  if (user_id == RGW_USER_ANON_ID) {
    err_msg = "unable to modify anonymous user's info";
    return false;
  }

  // if the user hasn't already been populated...attempt to
  if (!populated) {
    bool found = init(req);

    if (!found) {
      err_msg = "unable to retrieve user info";
      return false;
    }
  }

  email_check = strcmp(req.user_email.c_str(), user_info.user_email.c_str());
  same_email = (email_check== 0);

  // make sure we are not adding a duplicate email
  if (req.user_email_specified && !same_email) {
    ret = rgw_get_user_info_by_email(store, req.user_email, duplicate_check);
    if (ret >= 0) {
      err_msg = "cannot add duplicate email"; 
      return false;
    }

    user_info.user_email = req.user_email;
  }

  // update the remaining user info
  if (req.display_name_specified)
    user_info.display_name = req.display_name;

  if (req.max_buckets_specified)
    user_info.max_buckets = req.max_buckets;

  if (req.suspension_op) {
  
    string id;

    RGWUserBuckets buckets;
    if (rgw_read_user_buckets(store, user_id, buckets, false) < 0) {
      err_msg = "could not get buckets for uid:  " + user_id;
      return false;
    }
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    map<string, RGWBucketEnt>::iterator iter;

    vector<rgw_bucket> bucket_names;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      RGWBucketEnt obj = iter->second;
      bucket_names.push_back(obj.bucket);
    }
    ret = store->set_buckets_enabled(bucket_names, !req.is_suspended);
    if (ret < 0) {
     err_msg = "failed to change pool";
      return false;
    }
  }

  // if we're supposed to modify keys, do so 
  if (req.gen_access || req.id_specified || req.gen_secret || req.key_specified) {

    // the key parameters were already checked
    bool success = keys->execute_add(req, subprocess_msg, defer_save);
    if (!success) {
      err_msg = "unable to create or modify keys, " + subprocess_msg;
      return false;
    }
  }

  updated = update(err_msg);
  if (!updated)
    return false;

  return true;
}

bool RGWUser::modify(RGWUserAdminRequest &req, std::string &err_msg)
{
  std::string subprocess_msg;
  
  bool checked = check_request(req, subprocess_msg);
  if (!checked) {
    err_msg = "unable to parse parameters, " + subprocess_msg;
    return false;
  }

  bool modified = execute_modify(req, subprocess_msg);
  if (!modified) {
    err_msg = "unable to modify user, " + subprocess_msg;
    return false;
  }

  return true;
}

bool RGWUser::info(std::pair<uint32_t, std::string> id, RGWUserInfo &fetched_info, std::string &err_msg)
{
  std::string id_value = id.second;

  bool found = init(id);

  if (!found) {
    err_msg = "unable to fetch user info";
    return false;
  }

  // return the user info
  fetched_info = user_info;

  return true;
}

bool RGWUser::info(RGWUserAdminRequest &req, RGWUserInfo &fetched_info, std::string &err_msg)
{
  bool found = init(req);
  if (!found) {
    err_msg = "unable to fetch user info";
    return false;
  }

  // return the user info
  fetched_info = user_info;

  return true;
}

bool RGWUser::info(RGWUserInfo &fetched_info, std::string &err_msg)
{
  if (!populated) {
    err_msg = "no user info";
    return false;
  }

  if (failure) {
   err_msg = "previous error detected...aborting";
   return false;
  }

  // return the user info
  fetched_info = user_info;

  return true;
}

