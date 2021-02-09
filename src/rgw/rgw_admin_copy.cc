// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <iostream>

#include <boost/optional.hpp>

#include "rgw_user.h"
#include "rgw_rest_conn.h"
#include "rgw_admin_copy.h"

// It can be any non-existing zone ID.
#define SOURCE_ZONE_ID "dummy_source_zone"

void ObjEntry::decode_xml(XMLObj *obj)
{
  key.clear();
  etag.clear();
  size = 0;
  mtime = ceph::real_time();
  owner = Owner();

  RGWXMLDecoder::decode_xml("Key", key, obj, true);
  RGWXMLDecoder::decode_xml("ETag", etag, obj, true);
  RGWXMLDecoder::decode_xml("Size", size, obj, true);

  // The owner field is not present in listV2 by default.
  RGWXMLDecoder::decode_xml("Owner", owner, obj, false);

  string mtime_str;
  RGWXMLDecoder::decode_xml("LastModified", mtime_str, obj, true);
  boost::optional<ceph::real_time> date = ceph::from_iso_8601(mtime_str);
  if (date == boost::none) {
    throw RGWXMLDecoder::err("invalid LastModified value");
  }
  mtime = *date;
}

void ObjEntry::dump_xml(Formatter *f) const
{
  f->dump_string("Key", key);
  f->dump_string("ETag", etag);
  f->dump_int("Size", size);
  if (!owner.id.empty()) {
    f->open_array_section("Owner");
    owner.dump_xml(f);
    f->close_section();
  }

  string mtime_str;
  rgw_to_iso8601(mtime, &mtime_str);
  f->dump_string("LastModified", mtime_str);
}

void S3ListObjectsV2Resp::decode_xml(XMLObj *obj)
{
  common_prefixes.clear();
  contents.clear();
  continuation_token.clear();
  delimiter.clear();
  is_truncated = false;
  key_count = 0;
  max_keys = 0;
  name.clear();
  next_continuation_token.clear();
  prefix.clear();
  start_after.clear();

  // mandatory
  RGWXMLDecoder::decode_xml("Name", name, obj, true);
  RGWXMLDecoder::decode_xml("KeyCount", key_count, obj, true);
  RGWXMLDecoder::decode_xml("MaxKeys", max_keys, obj, true);
  RGWXMLDecoder::decode_xml("IsTruncated", is_truncated, obj, true);

  // optional
  RGWXMLDecoder::decode_xml("ContinuationToken", continuation_token, obj, false);
  RGWXMLDecoder::decode_xml("NextContinuationToken", next_continuation_token, obj, false);
  RGWXMLDecoder::decode_xml("Delimiter", delimiter, obj, false);
  RGWXMLDecoder::decode_xml("StartAfter", start_after, obj, false);
  RGWXMLDecoder::decode_xml("Prefix", prefix, obj, false);
  RGWXMLDecoder::decode_xml("CommonPrefixes", common_prefixes, obj, false);
  // Contents node may not exist when listing with prefix and delimiter.
  RGWXMLDecoder::decode_xml("Contents", contents, obj, false);
}

void S3ListObjectsV2Resp::dump_xml(Formatter *f) const
{
  f->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  f->dump_string("Name", name);
  f->dump_string("Prefix", prefix);
  f->dump_int("KeyCount", key_count);
  f->dump_int("MaxKeys", max_keys);
  f->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (!continuation_token.empty()) {
    f->dump_string("ContinuationToken", continuation_token);
  }
  if (!next_continuation_token.empty()) {
    f->dump_string("NextContinuationToken", next_continuation_token);
  }
  if (!delimiter.empty()) {
    f->dump_string("Delimiter", delimiter);
  }
  if (!start_after.empty()) {
    f->dump_string("StartAfter", start_after);
  }

  if (!common_prefixes.empty()) {
    f->open_array_section("CommonPrefixes");
    for (const auto &c : common_prefixes) {
      c.dump_xml(f);
    }
    f->close_section();
  }

  for (const auto &c : contents) {
    f->open_array_section("Contents");
    c.dump_xml(f);
    f->close_section();
  }

  f->close_section();
}

int BucketObjectsLister::get_next(unique_ptr<S3ListObjectsV2Resp> *resp)
{
  string resource("/" + bucket_name);
  param_vec_t params;
  params.push_back(param_pair_t("list-type", "2"));
  params.push_back(param_pair_t("max-keys", to_string(max_keys)));
  params.push_back(param_pair_t("fetch-owner", (fetch_owner ? "true" : "false")));
  if (!prefix.empty()) {
    params.push_back(param_pair_t("prefix", prefix));
  }
  if (!delimiter.empty()) {
    params.push_back(param_pair_t("delimiter", delimiter));
  }
  if (!continuation_token.empty()) {
    params.push_back(param_pair_t("continuation-token", continuation_token));
  }
  map<string, string> extra_headers;
  bufferlist in, out;

  int ret = conn->get_resource(resource, &params, &extra_headers, out, &in);
  if (ret < 0) {
    cerr << "ERROR: could not list objects: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWXMLParser parser;
  if (!parser.init()) {
    cerr << "ERROR: could not init XML parser" << std::endl;
    return -EINVAL;
  }

  if (!parser.parse(out.c_str(), out.length(), 1)) {
    cerr << "ERROR: could not parse list-objects-v2 XML response" << std::endl;
    return -ERR_MALFORMED_XML;
  }

  resp->reset(new S3ListObjectsV2Resp);
  try {
    RGWXMLDecoder::decode_xml("ListBucketResult", **resp, &parser);
  } catch (RGWXMLDecoder::err &err) {
    cerr << "ERROR: could not decode list-objects-v2 XML response: " << err << std::endl;
    return -ERR_MALFORMED_XML;
  }

  if (!(*resp)->next_continuation_token.empty()) {
    continuation_token = (*resp)->next_continuation_token;
  }

  return 0;
}

int copy_remote_bucket(RGWRados *store,
                       RGWBucketInfo &dest_bucket_info,
                       rgw_bucket &dest_bucket,
                       const string &tenant,
                       const string &bucket_name,
                       const list<string> &endpoints,
                       const RGWAccessKey &key)
{
  // RGWRESTConn for remote bucket fetching.
  RGWRESTConn *conn = nullptr;

  // We need to inject the RGWRESTConn for the remote cluster into the
  // RGWSI_Zone::zone_conn_map, so that RGWRados::fetch_remote_obj can be
  // reused, where it looks for the RGWRESTConn by source_zone when fetching
  // remote objects.
  map<string, RGWRESTConn *> &zone_conn_map = store->svc.zone->get_zone_conn_map();
  map<string, RGWRESTConn *>::iterator it = zone_conn_map.find(SOURCE_ZONE_ID);
  if (it != zone_conn_map.cend()) {
    conn = it->second;
  } else {
    conn = new RGWRESTConn(store->ctx(),
                           nullptr, // RGWSI_Zone *zone_svc
                           "", // const string& _remote_id
                           endpoints,
                           key);
    zone_conn_map[SOURCE_ZONE_ID] = conn;
  }

  BucketObjectsLister objLister(conn, bucket_name);

  while (true) {
    unique_ptr<S3ListObjectsV2Resp> listResp;
    int r = objLister.get_next(&listResp);
    if (r < 0) {
      return r;
    }

    XMLFormatter formatter(true);
    RGWStreamFlusher f(&formatter, cout);
    formatter.output_header();
    listResp->dump_xml(&formatter);
    formatter.output_footer();
    f.flush();

    rgw_bucket src_bucket;
    src_bucket.tenant = tenant;
    src_bucket.name = bucket_name;

    for (const auto &k : listResp->contents) {
      cerr << "INFO: copy remote object " << k.key << std::endl;

      rgw_obj src_obj(src_bucket, k.key);
      rgw_obj dest_obj(dest_bucket, k.key);

      RGWObjectCtx obj_ctx(store);
      string user_id;
      RGWBucketInfo src_bucket_info;
      std::optional<rgw_placement_rule> dest_placement_rule;
      std::optional<uint64_t> versioned_epoch;
      map<string, bufferlist> attrs;
      std::optional<uint64_t> bytes_transferred;

      r = store->fetch_remote_obj(obj_ctx,
                                  user_id,
                                  NULL,
                                  SOURCE_ZONE_ID,
                                  dest_obj,
                                  src_obj,
                                  dest_bucket_info,
                                  src_bucket_info,
                                  dest_placement_rule,
                                  NULL, /* real_time* src_mtime, */
                                  NULL, /* real_time* mtime, */
                                  NULL, /* const real_time* mod_ptr, */
                                  NULL, /* const real_time* unmod_ptr, */
                                  false, /* high precision time */
                                  NULL, /* const char *if_match, */
                                  NULL, /* const char *if_nomatch, */
                                  RGWRados::ATTRSMOD_NONE,
                                  true, /* copy_if_newer*/
                                  attrs,
                                  RGWObjCategory::Main,
                                  versioned_epoch,
                                  real_time(), /* delete_at */
                                  NULL, /* string *ptag, */
                                  NULL, /* string *petag, */
                                  NULL, /* void (*progress_cb)(off_t, void *), */
                                  NULL, /* void *progress_data*); */
                                  NULL, /* rgw_zone_set *zones_trace */
                                  &bytes_transferred);
      if (r < 0) {
        cerr << "ERROR: could not copy remote object " << k.key << std::endl;
        return r;
      }
    }

    if (!listResp->is_truncated) {
      return 0;
    }
  }

  return 0;
}
