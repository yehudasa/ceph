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

  string key;
  string etag;
  uint64_t size; // rgw_bucket_dir_entry_meta::accounted_size
  ceph::real_time mtime;

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

  Owner owner;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class S3ListObjectsResp
{
public:
  S3ListObjectsResp() {}
  ~S3ListObjectsResp() {}

  string name;
  string prefix;
  string delimiter;
  string marker;
  string next_marker;
  uint64 max_keys;
  bool is_truncated;
  vector<ObjEntry> contents;

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

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;

  string get_next_marker() const;
};

class BucketObjectsLister
{
protected:
  RGWRESTConn *conn;

  string bucket_name;
  uint64_t max_keys;

private:
  string marker;

public:
  BucketObjectsLister(RGWRESTConn *_conn,
		      const string &_bucket_name,
		      uint64_t _max_keys = LIST_OBJECTS_MAX_KEYS):
    conn(_conn),
    bucket_name(_bucket_name),
    max_keys(_max_keys) {}

  ~BucketObjectsLister() {}

  void set_marker(const string &_marker) {
    marker = _marker;
  }

  int get_next(unique_ptr<S3ListObjectsResp> *resp);
};

int copy_remote_bucket(RGWRados *store,
                       RGWBucketInfo &dest_bucket_info,
                       rgw_bucket &dest_bucket,
                       const string &tenant,
                       const string &bucket_name,
                       const list<string> &endpoints,
                       const RGWAccessKey &key);

#endif /*RGW_ADMIN_COPY_H */
