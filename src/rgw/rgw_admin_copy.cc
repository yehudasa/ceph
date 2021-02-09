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
  RGWXMLDecoder::decode_xml("Owner", owner, obj, true);

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
  f->open_array_section("Owner");
  owner.dump_xml(f);
  f->close_section();

  string mtime_str;
  rgw_to_iso8601(mtime, &mtime_str);
  f->dump_string("LastModified", mtime_str);
}

void S3ListObjectsResp::decode_xml(XMLObj *obj)
{
  name.clear();
  prefix.clear();
  delimiter.clear();
  marker.clear();
  next_marker.clear();
  max_keys = 0;
  is_truncated = false;
  common_prefixes.clear();
  contents.clear();

  // mandatory
  RGWXMLDecoder::decode_xml("Name", name, obj, true);
  RGWXMLDecoder::decode_xml("Prefix", prefix, obj, true);
  RGWXMLDecoder::decode_xml("MaxKeys", max_keys, obj, true);
  RGWXMLDecoder::decode_xml("IsTruncated", is_truncated, obj, true);
  RGWXMLDecoder::decode_xml("Contents", contents, obj, true);

  // optional
  RGWXMLDecoder::decode_xml("Marker", marker, obj, false);
  RGWXMLDecoder::decode_xml("NextMarker", next_marker, obj, false);
  RGWXMLDecoder::decode_xml("Delimiter", delimiter, obj, false);
  RGWXMLDecoder::decode_xml("CommonPrefixes", common_prefixes, obj, false);
}

void S3ListObjectsResp::dump_xml(Formatter *f) const
{
  f->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  f->dump_string("Name", name);
  f->dump_string("Prefix", prefix);
  f->dump_int("MaxKeys", max_keys);
  f->dump_string("Marker", marker);
  if (is_truncated && !next_marker.empty()) {
    f->dump_string("NextMarker", next_marker);
  }
  if (!delimiter.empty()) {
    f->dump_string("Delimiter", delimiter);
  }
  f->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  for (const auto &c : common_prefixes) {
    f->open_array_section("CommonPrefixes");
    c.dump_xml(f);
    f->close_section();
  }

  for (const auto &c : contents) {
    f->open_array_section("Contents");
    c.dump_xml(f);
    f->close_section();
  }

  f->close_section();
}

string S3ListObjectsResp::get_next_marker() const {
  if (!is_truncated) {
    return "";
  }

  if (next_marker.empty()) {
    return contents.back().key;
  }
  return next_marker;
}

int BucketObjectsLister::get_next(unique_ptr<S3ListObjectsResp> *resp)
{
  string resource("/" + bucket_name);
  param_vec_t params;
  params.push_back(param_pair_t("max-keys", to_string(max_keys)));
  if (!marker.empty()) {
    params.push_back(param_pair_t("marker", marker));
  }
  map<string, string> extra_headers;
  bufferlist in, out;

  int ret = conn->get_resource(resource, &params, &extra_headers, out, &in);
  if (ret < 0) {
    cerr << "ERROR: could not list bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWXMLParser parser;
  if (!parser.init()) {
    cerr << "ERROR: could not init XML parser" << std::endl;
    return -EINVAL;
  }

  if (!parser.parse(out.c_str(), out.length(), 1)) {
    cerr << "ERROR: could not parse list objects response" << std::endl;
    return -ERR_MALFORMED_XML;
  }

  resp->reset(new S3ListObjectsResp);
  try {
    RGWXMLDecoder::decode_xml("ListBucketResult", **resp, &parser);
  } catch (RGWXMLDecoder::err &err) {
    cerr << "ERROR: could not decode list objects XML response: " << err << std::endl;
    return -ERR_MALFORMED_XML;
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
  // Create RGWRESTConn for remote bucket fetching.
  RGWRESTConn *conn = new RGWRESTConn(store->ctx(),
                                      nullptr, // RGWSI_Zone *zone_svc
                                      "", // const string& _remote_id
                                      endpoints,
                                      key);

  // We need to inject the RGWRESTConn for the remote cluster into the
  // RGWSI_Zone::zone_conn_map, so that RGWRados::fetch_remote_obj can be
  // reused, where it looks for the RGWRESTConn by source_zone when fetching
  // remote objects.
  map<string, RGWRESTConn *> &zone_conn_map = store->svc.zone->get_zone_conn_map();
  map<string, RGWRESTConn *>::const_iterator it = zone_conn_map.find(SOURCE_ZONE_ID);
  if (it == zone_conn_map.cend()) {
    zone_conn_map[SOURCE_ZONE_ID] = conn;
  }

  BucketObjectsLister objLister(conn, bucket_name);

  while (true) {
    unique_ptr<S3ListObjectsResp> listResp;
    int r = objLister.get_next(&listResp);
    if (r < 0) {
      return r;
    }

    const string &marker = listResp->get_next_marker();
    if (marker.empty()) {
      cerr << "INFO: no more objects" << std::endl;
      return 0;
    }
    objLister.set_marker(marker);

    // XMLFormatter formatter(true);
    // RGWStreamFlusher f(&formatter, cout);
    // formatter.output_header();
    // listResp->dump_xml(&formatter);
    // formatter.output_footer();
    // f.flush();

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
  }

  return 0;
}
