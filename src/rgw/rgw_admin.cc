#include <errno.h>

#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "include/utime.h"
#include "include/str_list.h"

#include "common/armor.h"
#include "rgw_user.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "auth/Crypto.h"

#define dout_subsys ceph_subsys_rgw

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

RGWRados *store;

void _usage() 
{
  cerr << "usage: radosgw-admin <cmd> [options...]" << std::endl;
  cerr << "commands:\n";
  cerr << "  user create                create a new user\n" ;
  cerr << "  user modify                modify user\n";
  cerr << "  user info                  get user info\n";
  cerr << "  user rm                    remove user\n";
  cerr << "  user suspend               suspend a user\n";
  cerr << "  user enable                reenable user after suspension\n";
  cerr << "  caps add                   add user capabilities\n";
  cerr << "  caps rm                    remove user capabilities\n";
  cerr << "  subuser create             create a new subuser\n" ;
  cerr << "  subuser modify             modify subuser\n";
  cerr << "  subuser rm                 remove subuser\n";
  cerr << "  key create                 create access key\n";
  cerr << "  key rm                     remove access key\n";
  cerr << "  bucket list                list buckets\n";
  cerr << "  bucket link                link bucket to specified user\n";
  cerr << "  bucket unlink              unlink bucket from specified user\n";
  cerr << "  bucket stats               returns bucket statistics\n";
  cerr << "  bucket rm                  remove bucket\n";
  cerr << "  bucket check               check bucket index\n";
  cerr << "  object rm                  remove object\n";
  cerr << "  cluster info               show cluster params info\n";
  cerr << "  pool add                   add an existing pool for data placement\n";
  cerr << "  pool rm                    remove an existing pool from data placement set\n";
  cerr << "  pools list                 list placement active set\n";
  cerr << "  policy                     read bucket/object policy\n";
  cerr << "  log list                   list log objects\n";
  cerr << "  log show                   dump a log from specific object or (bucket + date\n";
  cerr << "                             + bucket-id)\n";
  cerr << "  log rm                     remove log object\n";
  cerr << "  usage show                 show usage (by user, date range)\n";
  cerr << "  usage trim                 trim usage (by user, date range)\n";
  cerr << "  temp remove                remove temporary objects that were created up to\n";
  cerr << "                             specified date (and optional time)\n";
  cerr << "  gc list                    dump expired garbage collection objects\n";
  cerr << "  gc process                 manually process garbage\n";
  cerr << "options:\n";
  cerr << "   --uid=<id>                user id\n";
  cerr << "   --subuser=<name>          subuser name\n";
  cerr << "   --access-key=<key>        S3 access key\n";
  cerr << "   --email=<email>\n";
  cerr << "   --secret=<key>            specify secret key\n";
  cerr << "   --gen-access-key          generate random access key (for S3)\n";
  cerr << "   --gen-secret              generate random secret key\n";
  cerr << "   --key-type=<type>         key type, options are: swift, s3\n";
  cerr << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cerr << "                             of read, write, readwrite, full\n";
  cerr << "   --display-name=<name>\n";
  cerr << "   --bucket=<bucket>\n";
  cerr << "   --pool=<pool>\n";
  cerr << "   --object=<object>\n";
  cerr << "   --date=<date>\n";
  cerr << "   --start-date=<date>\n";
  cerr << "   --end-date=<date>\n";
  cerr << "   --bucket-id=<bucket-id>\n";
  cerr << "   --fix                     besides checking bucket index, will also fix it\n";
  cerr << "   --format=<format>         specify output format for certain operations: xml,\n";
  cerr << "                             json\n";
  cerr << "   --purge-data              when specified, user removal will also purge all the\n";
  cerr << "                             user data\n";
  cerr << "   --purge-keys              when specified, subuser removal will also purge all the\n";
  cerr << "                             subuser keys\n";
  cerr << "   --purge-objects           remove a bucket's objects before deleting it\n";
  cerr << "                             (NOTE: required to delete a non-empty bucket)\n";
  cerr << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cerr << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cerr << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cerr << "                             in one of the numeric field\n";
  cerr << "   --categories=<list>       comma separated list of categories, used in usage show\n";
  cerr << "   --caps=<caps>             list of caps (e.g., \"usage=read, write; user=read\"\n";
  cerr << "   --yes-i-really-mean-it    required for certain operations\n";
  cerr << "\n";
  cerr << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cerr << "\n";
  generic_client_usage();
}

int usage()
{
  _usage();
  return 1;
}

void usage_exit()
{
  _usage();
  exit(1);
}

enum {
  OPT_NO_CMD = 0,
  OPT_USER_CREATE,
  OPT_USER_INFO,
  OPT_USER_MODIFY,
  OPT_USER_RM,
  OPT_USER_SUSPEND,
  OPT_USER_ENABLE,
  OPT_SUBUSER_CREATE,
  OPT_SUBUSER_MODIFY,
  OPT_SUBUSER_RM,
  OPT_KEY_CREATE,
  OPT_KEY_RM,
  OPT_BUCKETS_LIST,
  OPT_BUCKET_LINK,
  OPT_BUCKET_UNLINK,
  OPT_BUCKET_STATS,
  OPT_BUCKET_RM,
  OPT_BUCKET_CHECK,
  OPT_POLICY,
  OPT_POOL_ADD,
  OPT_POOL_RM,
  OPT_POOLS_LIST,
  OPT_LOG_LIST,
  OPT_LOG_SHOW,
  OPT_LOG_RM,
  OPT_USAGE_SHOW,
  OPT_USAGE_TRIM,
  OPT_TEMP_REMOVE,
  OPT_OBJECT_RM,
  OPT_GC_LIST,
  OPT_GC_PROCESS,
  OPT_CLUSTER_INFO,
  OPT_CAPS_ADD,
  OPT_CAPS_RM,
};

static uint32_t str_to_perm(const char *str)
{
  if (strcasecmp(str, "read") == 0)
    return RGW_PERM_READ;
  else if (strcasecmp(str, "write") == 0)
    return RGW_PERM_WRITE;
  else if (strcasecmp(str, "readwrite") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (strcasecmp(str, "full") == 0)
    return RGW_PERM_FULL_CONTROL;

  usage_exit();
  return 0; // unreachable
}

struct rgw_flags_desc {
  uint32_t mask;
  const char *str;
};

static struct rgw_flags_desc rgw_perms[] = {
 { RGW_PERM_FULL_CONTROL, "full-control" },
 { RGW_PERM_READ | RGW_PERM_WRITE, "read-write" },
 { RGW_PERM_READ, "read" },
 { RGW_PERM_WRITE, "write" },
 { RGW_PERM_READ_ACP, "read-acp" },
 { RGW_PERM_WRITE_ACP, "read-acp" },
 { 0, NULL }
};

static void perm_to_str(uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; rgw_perms[i].mask; i++) {
      struct rgw_flags_desc *desc = &rgw_perms[i];
      if ((mask & desc->mask) == desc->mask) {
        pos += snprintf(buf + pos, len - pos, "%s%s", sep, desc->str);
        if (pos == len)
          return;
        sep = ", ";
        mask &= ~desc->mask;
        if (!mask)
          return;
      }
    }
    if (mask == orig_mask) // no change
      break;
  }
}

static int get_cmd(const char *cmd, const char *prev_cmd, bool *need_more)
{
  *need_more = false;
  if (strcmp(cmd, "user") == 0 ||
      strcmp(cmd, "subuser") == 0 ||
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "bucket") == 0 ||
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "pools") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "usage") == 0 ||
      strcmp(cmd, "object") == 0 ||
      strcmp(cmd, "cluster") == 0 ||
      strcmp(cmd, "temp") == 0 ||
      strcmp(cmd, "caps") == 0 ||
      strcmp(cmd, "gc") == 0) {
    *need_more = true;
    return 0;
  }

  if (strcmp(cmd, "policy") == 0)
    return OPT_POLICY;

  if (!prev_cmd)
    return -EINVAL;

  if (strcmp(prev_cmd, "user") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_USER_CREATE;
    if (strcmp(cmd, "info") == 0)
      return OPT_USER_INFO;
    if (strcmp(cmd, "modify") == 0)
      return OPT_USER_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_USER_RM;
    if (strcmp(cmd, "suspend") == 0)
      return OPT_USER_SUSPEND;
    if (strcmp(cmd, "enable") == 0)
      return OPT_USER_ENABLE;
  } else if (strcmp(prev_cmd, "subuser") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_SUBUSER_CREATE;
    if (strcmp(cmd, "modify") == 0)
      return OPT_SUBUSER_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_SUBUSER_RM;
  } else if (strcmp(prev_cmd, "key") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_KEY_CREATE;
    if (strcmp(cmd, "rm") == 0)
      return OPT_KEY_RM;
  } else if (strcmp(prev_cmd, "buckets") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BUCKETS_LIST;
  } else if (strcmp(prev_cmd, "bucket") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BUCKETS_LIST;
    if (strcmp(cmd, "link") == 0)
      return OPT_BUCKET_LINK;
    if (strcmp(cmd, "unlink") == 0)
      return OPT_BUCKET_UNLINK;
    if (strcmp(cmd, "stats") == 0)
      return OPT_BUCKET_STATS;
    if (strcmp(cmd, "rm") == 0)
      return OPT_BUCKET_RM;
    if (strcmp(cmd, "check") == 0)
      return OPT_BUCKET_CHECK;
  } else if (strcmp(prev_cmd, "log") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_LOG_LIST;
    if (strcmp(cmd, "show") == 0)
      return OPT_LOG_SHOW;
    if (strcmp(cmd, "rm") == 0)
      return OPT_LOG_RM;
  } else if (strcmp(prev_cmd, "usage") == 0) {
    if (strcmp(cmd, "show") == 0)
      return OPT_USAGE_SHOW;
    if (strcmp(cmd, "trim") == 0)
      return OPT_USAGE_TRIM;
  } else if (strcmp(prev_cmd, "temp") == 0) {
    if (strcmp(cmd, "remove") == 0)
      return OPT_TEMP_REMOVE;
  } else if (strcmp(prev_cmd, "caps") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_CAPS_ADD;
    if (strcmp(cmd, "rm") == 0)
      return OPT_CAPS_RM;
  } else if (strcmp(prev_cmd, "pool") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_POOL_ADD;
    if (strcmp(cmd, "rm") == 0)
      return OPT_POOL_RM;
  } else if (strcmp(prev_cmd, "pools") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_POOLS_LIST;
  } else if (strcmp(prev_cmd, "object") == 0) {
    if (strcmp(cmd, "rm") == 0)
      return OPT_OBJECT_RM;
  } else if (strcmp(prev_cmd, "cluster") == 0) {
    if (strcmp(cmd, "info") == 0)
      return OPT_CLUSTER_INFO;
  } else if (strcmp(prev_cmd, "gc") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_GC_LIST;
    if (strcmp(cmd, "process") == 0)
      return OPT_GC_PROCESS;
  }

  return -EINVAL;
}

string escape_str(string& src, char c)
{
  int pos = 0;
  string s = src;
  string dest;

  do {
    int new_pos = src.find(c, pos);
    if (new_pos >= 0) {
      dest += src.substr(pos, new_pos - pos);
      dest += "\\";
      dest += c;
    } else {
      dest += src.substr(pos);
      return dest;
    }
    pos = new_pos + 1;
  } while (pos < (int)src.size());

  return dest;
}

static void show_user_info(RGWUserInfo& info, Formatter *formatter)
{
  map<string, RGWAccessKey>::iterator kiter;
  map<string, RGWSubUser>::iterator uiter;


  formatter->open_object_section("user_info");

  formatter->dump_string("user_id", info.user_id);
  formatter->dump_string("display_name", info.display_name);
  formatter->dump_string("email", info.user_email);
  formatter->dump_int("suspended", (int)info.suspended);
  formatter->dump_int("max_buckets", (int)info.max_buckets);

  // subusers
  formatter->open_array_section("subusers");
  for (uiter = info.subusers.begin(); uiter != info.subusers.end(); ++uiter) {
    RGWSubUser& u = uiter->second;
    formatter->open_object_section("user");
    formatter->dump_format("id", "%s:%s", info.user_id.c_str(), u.name.c_str());
    char buf[256];
    perm_to_str(u.perm_mask, buf, sizeof(buf));
    formatter->dump_string("permissions", buf);
    formatter->close_section();
    formatter->flush(cout);
  }
  formatter->close_section();

  // keys
  formatter->open_array_section("keys");
  for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    const char *sep = (k.subuser.empty() ? "" : ":");
    const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
    formatter->open_object_section("key");
    formatter->dump_format("user", "%s%s%s", info.user_id.c_str(), sep, subuser);
    formatter->dump_string("access_key", k.id);
    formatter->dump_string("secret_key", k.key);
    formatter->close_section();
  }
  formatter->close_section();

  formatter->open_array_section("swift_keys");
  for (kiter = info.swift_keys.begin(); kiter != info.swift_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    const char *sep = (k.subuser.empty() ? "" : ":");
    const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
    formatter->open_object_section("key");
    formatter->dump_format("user", "%s%s%s", info.user_id.c_str(), sep, subuser);
    formatter->dump_string("secret_key", k.key);
    formatter->close_section();
  }
  formatter->close_section();

  info.caps.dump(formatter);

  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
}

static int create_bucket(string bucket_str, string& user_id, string& display_name)
{
  RGWAccessControlPolicy policy, old_policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  string no_oid;
  rgw_obj obj;
  RGWBucketInfo bucket_info;

  int ret;

  // defaule policy (private)
  policy.create_default(user_id, display_name);
  policy.encode(aclbl);

  ret = store->get_bucket_info(NULL, bucket_str, bucket_info);
  if (ret < 0)
    return ret;

  rgw_bucket& bucket = bucket_info.bucket;

  ret = store->create_bucket(user_id, bucket, attrs);
  if (ret && ret != -EEXIST)   
    goto done;

  obj.init(bucket, no_oid);

  ret = store->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
  if (ret < 0) {
    cerr << "couldn't set acl on bucket" << std::endl;
  }

  ret = rgw_add_bucket(store, user_id, bucket);

  dout(20) << "ret=" << ret << dendl;

  if (ret == -EEXIST)
    ret = 0;
done:
  return ret;
}

static void dump_bucket_usage(map<RGWObjCategory, RGWBucketStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWBucketStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    formatter->dump_int("size_kb", s.num_kb);
    formatter->dump_int("size_kb_actual", s.num_kb_rounded);
    formatter->dump_int("num_objects", s.num_objects);
    formatter->close_section();
    formatter->flush(cout);
  }
  formatter->close_section();
}

int bucket_stats(rgw_bucket& bucket, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  int r = store->get_bucket_info(NULL, bucket.name, bucket_info);
  if (r < 0)
    return r;

  map<RGWObjCategory, RGWBucketStats> stats;
  int ret = store->get_bucket_stats(bucket, stats);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }
  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("pool", bucket.pool);
  
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_string("owner", bucket_info.owner);
  dump_bucket_usage(stats, formatter);
  formatter->close_section();

  return 0;
}

int main(int argc, char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::string user_id, access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object;
  std::string date, subuser, access, format;
  std::string start_date, end_date;
  std::string key_type_str;
  ObjectKeyType key_type = KEY_TYPE_S3;
  rgw_bucket bucket;
  uint32_t perm_mask = 0;
  RGWUserInfo info;
  int opt_cmd = OPT_NO_CMD;
  bool need_more;
  int gen_secret = false;
  int gen_key = false;
  string bucket_id;
  Formatter *formatter = NULL;
  int purge_data = false;
  RGWBucketInfo bucket_info;
  int pretty_format = false;
  int show_log_entries = true;
  int show_log_sum = true;
  int skip_zero_entries = false;  // log show
  int purge_keys = false;
  int yes_i_really_mean_it = false;
  int delete_child_objects = false;
  int fix = false;
  int max_buckets = -1;
  map<string, bool> categories;
  string caps;
  RGWUserAdminRequest req;

  std::string val;
  std::ostringstream errs;
  long long tmp = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*)NULL)) {
      user_id = val;
      req.user_id = user_id;
    } else if (ceph_argparse_witharg(args, i, &val, "--access-key", (char*)NULL)) {
      access_key = val;
      req.id = access_key;
    } else if (ceph_argparse_witharg(args, i, &val, "--subuser", (char*)NULL)) {
      subuser = val;
      req.subuser = subuser;
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", (char*)NULL)) {
      secret_key = val;
      req.key = secret_key;
    } else if (ceph_argparse_witharg(args, i, &val, "-e", "--email", (char*)NULL)) {
      user_email = val;
      req.user_email = user_email;
    } else if (ceph_argparse_witharg(args, i, &val, "-n", "--display-name", (char*)NULL)) {
      display_name = val;
      req.display_name = display_name;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", "--bucket", (char*)NULL)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      pool_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--object", (char*)NULL)) {
      object = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--key-type", (char*)NULL)) {
      key_type_str = val;
      if (key_type_str.compare("swift") == 0) {
        key_type = KEY_TYPE_SWIFT;
      } else if (key_type_str.compare("s3") == 0) {
        key_type = KEY_TYPE_S3;
      } else {
        cerr << "bad key type: " << key_type_str << std::endl;
        return usage();
      }

      req.key_type = key_type;
    } else if (ceph_argparse_binary_flag(args, i, &gen_key, NULL, "--gen-access-key", (char*)NULL)) {
      req.gen_access = true;
    } else if (ceph_argparse_binary_flag(args, i, &gen_secret, NULL, "--gen-secret", (char*)NULL)) {
      req.gen_secret = true;
    } else if (ceph_argparse_binary_flag(args, i, &show_log_entries, NULL, "--show_log_entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_sum, NULL, "--show_log_sum", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &skip_zero_entries, NULL, "--skip_zero_entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_withlonglong(args, i, &tmp, &errs, "-a", "--auth-uid", (char*)NULL)) {
      if (!errs.str().empty()) {
	cerr << errs.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*)NULL)) {
      max_buckets = atoi(val.c_str());
      req.max_buckets = max_buckets;
    } else if (ceph_argparse_witharg(args, i, &val, "--date", "--time", (char*)NULL)) {
      date = val;
      if (end_date.empty())
        end_date = date;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-date", "--start-time", (char*)NULL)) {
      start_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-date", "--end-time", (char*)NULL)) {
      end_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access", (char*)NULL)) {
      access = val;
      perm_mask = str_to_perm(access.c_str());
      req.perm_specified = true;
      req.perm_mask = perm_mask;
    } else if (ceph_argparse_witharg(args, i, &val, "--bucket-id", (char*)NULL)) {
      bucket_id = val;
      if (bucket_id.empty()) {
        cerr << "bad bucket-id" << std::endl;
        return usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      format = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--categories", (char*)NULL)) {
      string cat_str = val;
      list<string> cat_list;
      list<string>::iterator iter;
      get_str_list(cat_str, cat_list);
      for (iter = cat_list.begin(); iter != cat_list.end(); ++iter) {
	categories[*iter] = true;
      }
    } else if (ceph_argparse_binary_flag(args, i, &delete_child_objects, NULL, "--purge-objects", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &pretty_format, NULL, "--pretty-format", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &purge_data, NULL, "--purge-data", (char*)NULL)) {
      delete_child_objects = purge_data;
    } else if (ceph_argparse_binary_flag(args, i, &purge_keys, NULL, "--purge-keys", (char*)NULL)) {
      req.purge_keys = true;
    } else if (ceph_argparse_binary_flag(args, i, &yes_i_really_mean_it, NULL, "--yes-i-really-mean-it", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &fix, NULL, "--fix", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--caps", (char*)NULL)) {
      caps = val;
      req.caps.append(val);
    } else {
      ++i;
    }
  }

  if (args.size() == 0) {
    return usage();
  }
  else {
    const char *prev_cmd = NULL;
    for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
      opt_cmd = get_cmd(*i, prev_cmd, &need_more);
      if (opt_cmd < 0) {
	cerr << "unrecognized arg " << *i << std::endl;
	return usage();
      }
      if (!need_more)
	break;
      prev_cmd = *i;
    }
    if (opt_cmd == OPT_NO_CMD)
      return usage();
  }

  // default to pretty json
  if (format.empty()) {
    format = "json";
    pretty_format = true;
  }

  if (format ==  "xml")
    formatter = new XMLFormatter(pretty_format);
  else if (format == "json")
    formatter = new JSONFormatter(pretty_format);
  else {
    cerr << "unrecognized format: " << format << std::endl;
    return usage();
  }


  store = RGWStoreManager::get_storage(g_ceph_context, false);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  if (!bucket_name.empty()) {
    string bucket_name_str = bucket_name;
    RGWBucketInfo bucket_info;
    int r = store->get_bucket_info(NULL, bucket_name_str, bucket_info);
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name_str << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;
  }

  // RGWUser to use for user operations
  RGWUser *user;
  std::pair<int, string> id;

  // required to gather errors from operations
  std::string err_msg;

  // various condition flags
  bool created;
  bool modified;
  bool removed;
  bool fetched;

  switch (opt_cmd) {
  case OPT_USER_CREATE:
    // Create a new user object
    user = new RGWUser(store, req);

    created = user->add(req, err_msg);
    if (!created) {
      cerr << "could not create user: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_USER_RM:
    // Create a new user object
    user = new RGWUser(store, req);

    removed = user->remove(req, err_msg);
    if (!removed) {
      cerr << "could not remove user: " << err_msg << std::endl;
      return 1;
    }

    delete user;
    break;

  case OPT_USER_MODIFY:
    user = new RGWUser(store, req);

    modified = user->modify(req, err_msg);
    if (!modified) {
      cerr << "could not modify user: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_USER_ENABLE:
    req.suspension_op = true; // probably not needed
    req.is_suspended = 0;

    user = new RGWUser(store, req);

    modified = user->modify(req, err_msg);
    if (!modified) {
      cerr << "could not enable user: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_USER_SUSPEND:
    req.suspension_op = true; // probably not needed
    req.is_suspended = 1;

    user = new RGWUser(store, req);

    modified = user->modify(req, err_msg);
    if (!modified) {
      cerr << "could not suspend user: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;


  case OPT_SUBUSER_CREATE:
    user = new RGWUser(store, req);

    created = user->subusers->add(req, err_msg);
    if (!created) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_SUBUSER_MODIFY:
    user = new RGWUser(store, req);

    modified = user->subusers->modify(req, err_msg);
    if (!modified) {
      cerr << "could not modify subuser: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_SUBUSER_RM:
    user = new RGWUser(store, req);

    removed = user->subusers->remove(req, err_msg);
    if (!removed) {
      cerr << "could not remove subuser: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_CAPS_ADD:
    user = new RGWUser(store, req);

    created = user->caps->add(req, err_msg);
    if (!created) {
      cerr << "could not add caps: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_CAPS_RM:
    user = new RGWUser(store, req);

    removed = user->caps->remove(req, err_msg);
    if (!removed) {
      cerr << "could not add remove caps: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_KEY_CREATE:
    user = new RGWUser(store, req);

    created = user->keys->add(req, err_msg);
    if (!created) {
      cerr << "could not create key: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_KEY_RM:
    user = new RGWUser(store, req);

    removed = user->keys->remove(req, err_msg);
    if (!removed) {
      cerr << "could not remove key: " << err_msg << std::endl;
      return 1;
    }

    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;

  case OPT_USER_INFO:
    user = new RGWUser(store, req);


    fetched = user->info(info, err_msg);
    if (!fetched) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return 1;
    }

    show_user_info(info, formatter);

    delete user;
    break;
  }

  if (opt_cmd == OPT_POLICY) {
    bufferlist bl;
    rgw_obj obj(bucket, object);
    int ret = store->get_attr(NULL, obj, RGW_ATTR_ACL, bl);

    RGWAccessControlPolicy_S3 policy(g_ceph_context);
    if (ret >= 0) {
      bufferlist::iterator iter = bl.begin();
      try {
        policy.decode(iter);
      } catch (buffer::error& err) {
        dout(0) << "ERROR: caught buffer::error, could not decode policy" << dendl;
        return -EIO;
      }
      policy.to_xml(cout);
      cout << std::endl;
    }
  }

  if (opt_cmd == OPT_BUCKETS_LIST) {
    RGWAccessHandle handle;

    formatter->reset();
    formatter->open_array_section("buckets");
    if (!user_id.empty()) {
      RGWUserBuckets buckets;
      if (rgw_read_user_buckets(store, user_id, buckets, false) < 0) {
        cerr << "list buckets: could not get buckets for uid " << user_id << std::endl;
      } else {
        map<string, RGWBucketEnt>& m = buckets.get_buckets();
        map<string, RGWBucketEnt>::iterator iter;

        for (iter = m.begin(); iter != m.end(); ++iter) {
          RGWBucketEnt obj = iter->second;
	  formatter->dump_string("bucket", obj.bucket.name);
        }
      }
    } else {
      if (store->list_buckets_init(&handle) < 0) {
        cerr << "list buckets: no buckets found" << std::endl;
      } else {
        RGWObjEnt obj;
        while (store->list_buckets_next(obj, &handle) >= 0) {
          formatter->dump_string("bucket", obj.name);
        }
      }
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
    if (bucket_name.empty()) {
      cerr << "bucket name was not specified" << std::endl;
      return usage();
    }
    string uid_str(user_id);
    
    string no_oid;
    bufferlist aclbl;
    rgw_obj obj(bucket, no_oid);

    int r = store->get_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
    if (r >= 0) {
      RGWAccessControlPolicy policy;
      ACLOwner owner;
      try {
       bufferlist::iterator iter = aclbl.begin();
       ::decode(policy, iter);
       owner = policy.get_owner();
      } catch (buffer::error& err) {
	dout(10) << "couldn't decode policy" << dendl;
	return -EINVAL;
      }
      //cout << "bucket is linked to user '" << owner.get_id() << "'.. unlinking" << std::endl;
      r = rgw_remove_user_bucket_info(store, owner.get_id(), bucket);
      if (r < 0) {
        cerr << "could not unlink policy from user '" << owner.get_id() << "'" << std::endl;
        return r;
      }

      // now update the user for the bucket...
      if (info.display_name.empty()) {
        cerr << "WARNING: user " << info.user_id << " has no display name set" << std::endl;
      } else {
        policy.create_default(info.user_id, info.display_name);

        // ...and encode the acl
        aclbl.clear();
        policy.encode(aclbl);

        r = store->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
        if (r < 0)
          return r;

        r = rgw_add_bucket(store, info.user_id, bucket);
        if (r < 0)
          return r;
      }
    } else {
      // the bucket seems not to exist, so we should probably create it...
      r = create_bucket(bucket_name.c_str(), uid_str, info.display_name);
      if (r < 0)
        cerr << "error linking bucket to user: r=" << r << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    if (bucket_name.empty()) {
      cerr << "bucket name was not specified" << std::endl;
      return usage();
    }

    int r = rgw_remove_user_bucket_info(store, user_id, bucket);
    if (r < 0)
      cerr << "error unlinking bucket " <<  cpp_strerror(-r) << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_TEMP_REMOVE) {
    if (date.empty()) {
      cerr << "date wasn't specified" << std::endl;
      return usage();
    }
    string parsed_date, parsed_time;
    int r = parse_date(date, NULL, &parsed_date, &parsed_time);
    if (r < 0) {
      cerr << "failure parsing date: " << cpp_strerror(r) << std::endl;
      return 1;
    }
    r = store->remove_temp_objects(parsed_date, parsed_time);
    if (r < 0) {
      cerr << "failure removing temp objects: " << cpp_strerror(r) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_LOG_LIST) {
    // filter by date?
    if (date.size() && date.size() != 10) {
      cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
      return -EINVAL;
    }

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = store->log_list_init(date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
	cerr << "log list: error " << r << std::endl;
	return r;
      }
      while (true) {
	string name;
	int r = store->log_list_next(h, &name);
	if (r == -ENOENT)
	  break;
	if (r < 0) {
	  cerr << "log list: error " << r << std::endl;
	  return r;
	}
	formatter->dump_string("object", name);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_LOG_SHOW || opt_cmd == OPT_LOG_RM) {
    if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
      cerr << "object or (at least one of date, bucket, bucket-id) were not specified" << std::endl;
      return usage();
    }

    string oid;
    if (!object.empty()) {
      oid = object;
    } else {
      oid = date;
      oid += "-";
      oid += bucket_id;
      oid += "-";
      oid += string(bucket.name);
    }

    if (opt_cmd == OPT_LOG_SHOW) {
      RGWAccessHandle h;

      int r = store->log_show_init(oid, &h);
      if (r < 0) {
	cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;
      
      // peek at first entry to get bucket metadata
      r = store->log_show_next(h, &entry);
      if (r < 0) {
	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      formatter->dump_string("bucket_id", entry.bucket_id);
      formatter->dump_string("bucket_owner", entry.bucket_owner);
      formatter->dump_string("bucket", entry.bucket);

      uint64_t agg_time = 0;
      uint64_t agg_bytes_sent = 0;
      uint64_t agg_bytes_received = 0;
      uint64_t total_entries = 0;

      if (show_log_entries)
        formatter->open_array_section("log_entries");

      do {
	uint64_t total_time =  entry.total_time.sec() * 1000000LL * entry.total_time.usec();

        agg_time += total_time;
        agg_bytes_sent += entry.bytes_sent;
        agg_bytes_received += entry.bytes_received;
        total_entries++;

        if (skip_zero_entries && entry.bytes_sent == 0 &&
            entry.bytes_received == 0)
          goto next;

        if (show_log_entries) {
	  formatter->open_object_section("log_entry");
	  formatter->dump_string("bucket", entry.bucket);
	  entry.time.gmtime(formatter->dump_stream("time"));      // UTC
	  entry.time.localtime(formatter->dump_stream("time_local"));
	  formatter->dump_string("remote_addr", entry.remote_addr);
	  if (entry.object_owner.length())
	    formatter->dump_string("object_owner", entry.object_owner);
	  formatter->dump_string("user", entry.user);
	  formatter->dump_string("operation", entry.op);
	  formatter->dump_string("uri", entry.uri);
	  formatter->dump_string("http_status", entry.http_status);
	  formatter->dump_string("error_code", entry.error_code);
	  formatter->dump_int("bytes_sent", entry.bytes_sent);
	  formatter->dump_int("bytes_received", entry.bytes_received);
	  formatter->dump_int("object_size", entry.obj_size);
	  formatter->dump_int("total_time", total_time);
	  formatter->dump_string("user_agent",  entry.user_agent);
	  formatter->dump_string("referrer",  entry.referrer);
	  formatter->close_section();
	  formatter->flush(cout);
        }
next:
	r = store->log_show_next(h, &entry);
      } while (r > 0);

      if (r < 0) {
      	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      if (show_log_entries)
        formatter->close_section();

      if (show_log_sum) {
        formatter->open_object_section("log_sum");
	formatter->dump_int("bytes_sent", agg_bytes_sent);
	formatter->dump_int("bytes_received", agg_bytes_received);
	formatter->dump_int("total_time", agg_time);
	formatter->dump_int("total_entries", total_entries);
        formatter->close_section();
      }
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    if (opt_cmd == OPT_LOG_RM) {
      int r = store->log_remove(oid);
      if (r < 0) {
	cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
    }
  }
  
  
  if (opt_cmd == OPT_POOL_ADD) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      return usage();
    }

    int ret = store->add_bucket_placement(pool_name);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOL_RM) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      return usage();
    }

    int ret = store->remove_bucket_placement(pool_name);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOLS_LIST) {
    set<string> pools;
    int ret = store->list_placement_set(pools);
    if (ret < 0) {
      cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    formatter->reset();
    formatter->open_array_section("pools");
    set<string>::iterator siter;
    for (siter = pools.begin(); siter != pools.end(); ++siter) {
      formatter->open_object_section("pool");
      formatter->dump_string("name",  *siter);
      formatter->close_section();
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_BUCKET_STATS) {
    if (bucket_name.empty() && user_id.empty()) {
      cerr << "either bucket or uid needs to be specified" << std::endl;
      return usage();
    }
    formatter->reset();
    if (user_id.empty()) {
      bucket_stats(bucket, formatter);
    } else {
      RGWUserBuckets buckets;
      if (rgw_read_user_buckets(store, user_id, buckets, false) < 0) {
	cerr << "could not get buckets for uid " << user_id << std::endl;
      } else {
	formatter->open_array_section("buckets");
	map<string, RGWBucketEnt>& m = buckets.get_buckets();
	for (map<string, RGWBucketEnt>::iterator iter = m.begin(); iter != m.end(); ++iter) {
	  RGWBucketEnt obj = iter->second;
	  bucket_stats(obj.bucket, formatter);
	}
	formatter->close_section();
      }
    }
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_USAGE_SHOW) {
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;

    int ret;
    
    if (!start_date.empty()) {
      ret = parse_date(start_date, &start_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }
    if (!end_date.empty()) {
      ret = parse_date(end_date, &end_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    RGWStreamFlusher f(formatter, cout);

    ret = RGWUsage::show(store, user_id, start_epoch, end_epoch,
			 show_log_entries, show_log_sum, &categories,
			 f);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_USAGE_TRIM) {
    if (user_id.empty() && !yes_i_really_mean_it) {
      cerr << "usage trim without user specified will remove *all* users data" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }
    int ret;
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;


    if (!start_date.empty()) {
      ret = parse_date(start_date, &start_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }

    if (!end_date.empty()) {
      ret = parse_date(end_date, &end_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    ret = RGWUsage::trim(store, user_id, start_epoch, end_epoch);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }   
  }

  if (opt_cmd == OPT_OBJECT_RM) {
    int ret = remove_object(store, bucket, object);

    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_BUCKET_CHECK) {
    map<RGWObjCategory, RGWBucketStats> existing_stats;
    map<RGWObjCategory, RGWBucketStats> calculated_stats;

    int r = store->bucket_check_index(bucket, &existing_stats, &calculated_stats);
    if (r < 0) {
      cerr << "failed to check index err=" << cpp_strerror(-r) << std::endl;
      return r;
    }

    formatter->open_object_section("check_result");
    formatter->open_object_section("existing_header");
    dump_bucket_usage(existing_stats, formatter);
    formatter->close_section();
    formatter->open_object_section("calculated_header");
    dump_bucket_usage(calculated_stats, formatter);
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

    if (fix) {
      r = store->bucket_rebuild_index(bucket);
      if (r < 0) {
        cerr << "failed to rebuild index err=" << cpp_strerror(-r) << std::endl;
        return r;
      }
    }
  }

  if (opt_cmd == OPT_BUCKET_RM) {
    int ret = remove_bucket(store, bucket, delete_child_objects);

    if (ret < 0) {
      cerr << "ERROR: bucket remove returned: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_GC_LIST) {
    int ret;
    int index = 0;
    string marker;
    bool truncated;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      ret = store->list_gc_objs(&index, marker, 1000, result, &truncated);
      if (ret < 0) {
	cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
	return 1;
      }


      list<cls_rgw_gc_obj_info>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
	cls_rgw_gc_obj_info& info = *iter;
	formatter->open_object_section("chain_info");
	formatter->dump_string("tag", info.tag);
	formatter->dump_stream("time") << info.time;
	formatter->open_array_section("objs");
        list<cls_rgw_obj>::iterator liter;
	cls_rgw_obj_chain& chain = info.chain;
	for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
	  cls_rgw_obj& obj = *liter;
	  formatter->dump_string("pool", obj.pool);
	  formatter->dump_string("oid", obj.oid);
	  formatter->dump_string("key", obj.key);
	}
	formatter->close_section(); // objs
	formatter->close_section(); // obj_chain
	formatter->flush(cout);
      }
    } while (truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_GC_PROCESS) {
    int ret = store->process_gc();
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_CLUSTER_INFO) {
    store->params.dump(formatter);
    formatter->flush(cout);
  }
  return 0;
}
