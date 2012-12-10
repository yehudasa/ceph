#ifndef CEPH_RGW_USER_H
#define CEPH_RGW_USER_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

using namespace std;

#define RGW_USER_ANON_ID "anonymous"

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

/**
 * A string wrapper that includes encode/decode functions
 * for easily accessing a UID in all forms
 */
struct RGWUID
{
  string user_id;
  void encode(bufferlist& bl) const {
    ::encode(user_id, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(user_id, bl);
  }
};
WRITE_CLASS_ENCODER(RGWUID)

/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
extern void rgw_get_anon_user(RGWUserInfo& info);

/**
 * verify that user is an actual user, and not the anonymous user
 */
extern bool rgw_user_is_authenticated(RGWUserInfo& info);
/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_store_user_info(RGWRados *store, RGWUserInfo& info, bool exclusive);
/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_uid(RGWRados *store, string& user_id, RGWUserInfo& info);
/**
 * Given an swift username, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_email(RGWRados *store, string& email, RGWUserInfo& info);
/**
 * Given an swift username, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados *store, string& swift_name, RGWUserInfo& info);
/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados *store, string& access_key, RGWUserInfo& info);
/**
 * Given an RGWUserInfo, deletes the user and its bucket ACLs.
 */
extern int rgw_delete_user(RGWRados *store, RGWUserInfo& user);
/**
 * Store a list of the user's buckets, with associated functinos.
 */
class RGWUserBuckets
{
  map<string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() {}
  void encode(bufferlist& bl) const {
    ::encode(buckets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(buckets, bl);
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
  void add(RGWBucketEnt& bucket) {
    buckets[bucket.bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(string& name) {
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

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets, bool need_stats);

/**
 * Store the set of buckets associated with a user.
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
extern int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets);

extern int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket);
extern int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket);

/*
 * remove the different indexes
 */
extern int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key);
extern int rgw_remove_uid_index(RGWRados *store, string& uid);
extern int rgw_remove_email_index(RGWRados *store, string& email);
extern int rgw_remove_swift_name_index(RGWRados *store, string& swift_name);


/* remove these when changes complete */
extern bool validate_access_key(string& key);
extern int remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object);
extern int remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);


/* end remove these */
/*
 * An RGWUser class along with supporting classes created
 * to support the creation of an RESTful administrative API
 */

enum ObjectKeyType {
  KEY_TYPE_SWIFT,
  KEY_TYPE_S3,
};

enum RGWKeyPoolOp {
  CREATE_KEY,
  GENERATE_KEY,
  MODIFY_KEY
};

enum RGWUserId {
  RGW_USER_ID,
  RGW_SWIFT_USERNAME,
  RGW_USER_EMAIL,
  RGW_ACCESS_KEY,
};

struct RGWUserAdminRequest {
  
  // user attributes
  std::string user_id;
  std::string user_email;
  std::string display_name;
  uint32_t max_buckets;
  __u8 is_suspended; // not usually set manually
  std::string caps;
  
  // subuser attributes
  std::string subuser;
  uint32_t perm_mask;
  
  // key_attributes
  std::string id; // access key
  std::string key; // secret key
  int32_t key_type;
  
  // operation attributes
  bool existing_user;
  bool existing_key;
  bool existing_subuser;
  bool subuser_specified;
  bool purge_keys;
  bool gen_secret;
  bool gen_access;
  bool id_specified;
  bool key_specified;
  bool type_specified;
  bool purge_data;
  bool display_name_specified;
  bool user_email_specified;
  bool max_buckets_specified;
  bool perm_specified;
  bool suspension_op;
  uint32_t key_op;
};


/* 
 * There shouldn't really be a need to call this function since the RGWUserAdminRequest struct is the 
 * basis of all RGWUser admin operations, but could be useful for building a request from a strings
 * pulled from a socket or similar....
 */
bool rgw_build_user_request_from_map(map<std::string, std::string> request_parameters, RGWUserAdminRequest &_req);

class RGWUser;

class RGWAccessKeyPool 
{
  std::map<std::string, int, ltstr_nocase> key_type_map;
  std::string user_id;
  RGWRados *store;
  RGWUser *user;

  map<std::string, RGWAccessKey> *swift_keys;
  map<std::string, RGWAccessKey> *access_keys;

  // we don't want to allow keys for the anonymous user or a null user
  bool keys_allowed;

private:

  bool create_key(RGWUserAdminRequest req, string &err_msg);
  bool generate_key(RGWUserAdminRequest req, string &err_msg);
  bool modify_key(RGWUserAdminRequest req, string &err_msg);

  bool check_existing_key(RGWUserAdminRequest req);
  bool check_request(RGWUserAdminRequest req, string &err_msg);

  /* API Contract Fulfilment */
  bool execute_add(RGWUserAdminRequest req, string &err_msg, bool defer_save);
  bool execute_remove(RGWUserAdminRequest req, string &err_msg, bool defer_save);

  friend class RGWUser;
  friend class RGWSubUserPool;

public:

  RGWAccessKeyPool(RGWUser *user);
  ~RGWAccessKeyPool();


  /* API Contracted Methods */
  bool add(RGWUserAdminRequest, string &err_msg);
  bool remove(RGWUserAdminRequest, string &err_msg);
};

class RGWSubUserPool
{
  string user_id;
  RGWRados *store;
  RGWUser *user;
  bool subusers_allowed;

  map<string, RGWSubUser> *subuser_map;

private:
  bool check_request(RGWUserAdminRequest req, string &err_msg);

  /* API Contract Fulfilment */
  bool execute_add(RGWUserAdminRequest req, string &err_msg, bool defer_save);
  bool execute_remove(RGWUserAdminRequest req, string &err_msg, bool defer_save);
  bool execute_modify(RGWUserAdminRequest req, string &err_msg, bool defer_save);

public:
  bool exists(std::string subuser);

  RGWSubUserPool(RGWUser *rgw_user);
  ~RGWSubUserPool();

  /* API contracted methods */
  bool add(RGWUserAdminRequest req, string &err_msg);
  bool remove(RGWUserAdminRequest req, string &err_msg);
  bool modify(RGWUserAdminRequest req, string &err_msg);

  friend class RGWUser;
};


class RGWUserCapPool
{
  RGWUser *user;
  RGWUserCaps *caps;
  bool caps_allowed;
  
private:
public:

  RGWUserCapPool(RGWUser *user);
  ~RGWUserCapPool();

  bool add(RGWUserAdminRequest req, std::string &err_msg);
  bool remove(RGWUserAdminRequest req, std::string &err_msg);

  friend class RGWUser;
};

class RGWUser
{

private:
  RGWUserInfo user_info;
  RGWUserInfo old_info;
  RGWRados *store;

  string user_id;
  bool failure;
  bool populated;

  void set_failure() { failure = true; };
  bool check_request(RGWUserAdminRequest req, string &err_msg);
  bool update(std::string &err_msg);
 
  /* API Contract Fulfilment */
  bool execute_add(RGWUserAdminRequest req, string &err_msg);
  bool execute_remove(RGWUserAdminRequest req, string &err_msg);
  bool execute_modify(RGWUserAdminRequest req, string &err_msg);

public:
  RGWUser(RGWRados *_store, RGWUserAdminRequest req);
  RGWUser(RGWRados *_store, pair<int, string> user);
  RGWUser(RGWRados *_store);
  RGWUser();
  ~RGWUser();

  bool init(pair<int, string> user);
  bool init(RGWUserAdminRequest req);


  bool is_populated() { return populated; };
  bool has_failed() { return failure; };

  /* API Contracted Members */
  RGWUserCapPool *caps;
  RGWAccessKeyPool *keys;
  RGWSubUserPool *subusers;

  /* API Contracted Methods */
  bool add(RGWUserAdminRequest req, string &err_msg);
  bool remove(RGWUserAdminRequest req, string &err_msg);
  bool modify(RGWUserAdminRequest req, string &err_msg);
  bool info (std::pair<uint32_t, std::string> id, RGWUserInfo &fetched_info, string &err_msg);
  bool info (RGWUserAdminRequest req, RGWUserInfo &fetched_info, string &err_msg);
  bool info (RGWUserInfo &fetched_info, string &err_msg);


  friend class RGWAccessKeyPool;
  friend class RGWSubUserPool;
  friend class RGWUserCapPool;
};

#endif
