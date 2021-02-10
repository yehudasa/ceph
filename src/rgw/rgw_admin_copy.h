// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_ADMIN_COPY_H
#define RGW_ADMIN_COPY_H

#include <string>

#include "common/Formatter.h"
#include "common/errno.h"

#define LIST_OBJECTS_MAX_KEYS 100

class ObjEntry
{
public:
  ObjEntry() {}
  ~ObjEntry() {}

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
  uint64_t size; // rgw_bucket_dir_entry_meta::accounted_size
  ceph::real_time mtime;
  Owner owner;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class S3ListObjectsV2Resp
{
public:
  S3ListObjectsV2Resp() {}
  ~S3ListObjectsV2Resp() {}

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
  vector<ObjEntry> contents;
  string continuation_token;
  string delimiter;
  bool is_truncated;
  uint32_t key_count;
  uint32_t max_keys;
  string name;
  string next_continuation_token;
  string prefix;
  string start_after;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class BucketObjectsLister
{
protected:
  RGWRESTConn *conn{nullptr};

  string bucket_name;
  string start_after;
  string prefix;
  string delimiter;
  uint32_t max_keys;
  bool fetch_owner;

private:
  string continuation_token;

public:
  BucketObjectsLister(RGWRESTConn *_conn,
                      const string &_bucket_name,
                      const string &_start_after = "",
                      const string &_prefix = "",
                      const string &_delimiter = "",
                      uint32_t _max_keys = LIST_OBJECTS_MAX_KEYS,
                      bool _fetch_owner = false):
    conn(_conn),
    bucket_name(_bucket_name),
    start_after(_start_after),
    prefix(_prefix),
    delimiter(_delimiter),
    max_keys(_max_keys),
    fetch_owner(_fetch_owner) {}

  ~BucketObjectsLister() {}

  int get_next(unique_ptr<S3ListObjectsV2Resp> *resp);
};

int copy_remote_bucket(RGWRados *store,
                       RGWBucketInfo &dest_bucket_info,
                       rgw_bucket &dest_bucket,
                       const string &tenant,
                       const string &bucket_name,
                       const string &start_after,
                       const string &object_prefix,
                       const list<string> &endpoints,
                       const RGWAccessKey &key);

#endif /*RGW_ADMIN_COPY_H */
