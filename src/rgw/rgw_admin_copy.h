// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_ADMIN_COPY_H
#define RGW_ADMIN_COPY_H

#include <string>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/perf_counters_collection.h"

// It can be any non-existing zone ID.
#define BUCKET_COPY_SOURCE_ZONE_ID "bucket-copy-source-zone"

namespace bucket_copy {

class S3ListObjectsEntry
{
public:
  S3ListObjectsEntry() {}
  ~S3ListObjectsEntry() {}

  struct Owner
  {
    string id;
    string display_name;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("ID", id, obj, true);
      RGWXMLDecoder::decode_xml("DisplayName", display_name, obj, true);
    }

    void dump_xml(Formatter *f) const {
      f->dump_string("ID", id);
      f->dump_string("DisplayName", display_name);
    }
  };

  string key;
  string etag;
  uint64_t size{0}; // rgw_bucket_dir_entry_meta::accounted_size
  ceph::real_time mtime;
  Owner owner;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class S3ListObjectsResp
{
public:
  S3ListObjectsResp() {}
  ~S3ListObjectsResp() {}

  struct CommonPrefix
  {
    string value;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Prefix", value, obj, true);
    }

    void dump_xml(Formatter *f) const {
      f->dump_string("Prefix", value);
    }
  };

  vector<CommonPrefix> common_prefixes;
  vector<S3ListObjectsEntry> contents;
  string delimiter;
  bool is_truncated;
  uint32_t max_keys{0};
  string name;
  string prefix;
  string marker;
  string next_marker;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class BucketObjectsLister
{
protected:
  CephContext *cct{nullptr};
  RGWRESTConn *conn{nullptr};

  string bucket_name;
  string prefix;
  string delimiter;
  bool fetch_owner;

  bool allow_unordered;

private:
  bool is_truncated;
  string next_marker;
  string last_obj_key;

public:
  BucketObjectsLister(CephContext *_cct,
                      RGWRESTConn *_conn,
                      const string &_bucket_name,
                      const string &_prefix = "",
                      const string &_delimiter = "",
                      bool _fetch_owner = false,
                      bool _allow_unordered = false):
    cct(_cct),
    conn(_conn),
    bucket_name(_bucket_name),
    prefix(_prefix),
    delimiter(_delimiter),
    fetch_owner(_fetch_owner),
    allow_unordered(_allow_unordered) {}

  ~BucketObjectsLister() {}

  const std::string& get_next_token() const;
  int fetch_next(unique_ptr<S3ListObjectsResp> *resp, uint64_t max_keys);
};

class Stats
{
private:
  Stats(const Stats &rhs);
  Stats& operator=(const Stats &rhs);

  std::unique_ptr<PerfCounters> logger;

public:

  enum {
    l_first = 101010,

    l_copy_ok,
    l_copy_not_found,
    l_copy_err,

    l_bytes_transferred,

    l_last,
  };

  Stats();
  ~Stats();

  void dump(Formatter *f) const {
    if (logger) {
      logger->dump_formatted(f, false);
    }
  }

  void count(int r, uint64_t bytes_transferred);
  void reset();
};

int copy_remote_bucket(RGWRados *store,
                       RGWBucketInfo &dest_bucket_info,
                       rgw_bucket &dest_bucket,
                       const string &tenant,
                       const string &bucket_name,
                       const string &object_prefix,
                       const list<string> &endpoints,
                       const RGWAccessKey &key);

} // namespace bucket_copy

#endif /*RGW_ADMIN_COPY_H */
