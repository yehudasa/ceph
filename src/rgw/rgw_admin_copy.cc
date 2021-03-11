// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
#include <iostream>

#include <boost/optional.hpp>

#include "common/dout.h"

#include "rgw_user.h"
#include "rgw_rest_conn.h"
#include "rgw_admin_copy.h"

#define dout_subsys ceph_subsys_rgw
#define list_objects_attempts 5
#define list_objects_retry_sleep_seconds 5

void DO::S3ListObjectsV2Entry::decode_xml(XMLObj *obj)
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

void DO::S3ListObjectsV2Entry::dump_xml(Formatter *f) const
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

void DO::S3ListObjectsV2Resp::decode_xml(XMLObj *obj)
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

void DO::S3ListObjectsV2Resp::dump_xml(Formatter *f) const
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

int DO::BucketObjectsLister::fetch_next(unique_ptr<S3ListObjectsV2Resp> *resp, uint64_t max_keys)
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
  if (!start_after.empty()) {
    params.push_back(param_pair_t("start-after", start_after));
  }
  if (!continuation_token.empty()) {
    params.push_back(param_pair_t("continuation-token", continuation_token));
  }

  // RGW internal parameters
  params.push_back(param_pair_t("allow-unordered", (allow_unordered ? "true" : "false")));

  map<string, string> extra_headers;
  bufferlist in, out;

  int ret = conn->get_resource(resource, &params, &extra_headers, out, &in);
  if (ret < 0) {
    return ret;
  }

  RGWXMLParser parser;
  if (!parser.init()) {
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
    cerr << "ERROR: could not decode ListBucketResult: " << err << std::endl;
    return -ERR_MALFORMED_XML;
  }

  if (!(*resp)->next_continuation_token.empty()) {
    continuation_token = (*resp)->next_continuation_token;
  }

  return 0;
}

int DO::copy_remote_bucket(RGWRados *store,
                           RGWBucketInfo &dest_bucket_info,
                           rgw_bucket &dest_bucket,
                           const string &tenant,
                           const string &bucket_name,
                           const string &start_after,
                           const string &object_prefix,
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
  map<string, RGWRESTConn *>::iterator it = zone_conn_map.find(DO_BUCKET_COPY_SOURCE_ZONE_ID);

  // Though this should never happen, we ensure it does not already exist.
  if (it != zone_conn_map.cend()) {
    cerr << "ERROR: source zone " << DO_BUCKET_COPY_SOURCE_ZONE_ID << "already exists" << std::endl;
    return -EEXIST;
  }

  conn = new RGWRESTConn(store->ctx(),
                         nullptr, // RGWSI_Zone *zone_svc
                         "", // const string& _remote_id
                         endpoints,
                         key);
  zone_conn_map[DO_BUCKET_COPY_SOURCE_ZONE_ID] = conn;


  DO::BucketObjectsLister lister(conn, bucket_name, start_after, object_prefix);
  DO::BucketCopyStats stats(bucket_name);

  while (true) {
    auto copy_batch_num = g_conf().get_val<uint64_t>("rgw_bucket_copy_batch_num");

    ldout(store->ctx(), 10) << "list remote bucket=" << bucket_name
                            << ", max_keys=" << copy_batch_num
                            << ", continuation_token=" << lister.get_continuation_token()
                            << dendl;

    unique_ptr<DO::S3ListObjectsV2Resp> listResp;

    for (int i = 1; i <= list_objects_attempts; i++) {
      int ret = lister.fetch_next(&listResp, copy_batch_num);
      if (ret < 0) {
        cerr << "ERROR: could not list bucket: " << bucket_name
             << ", max_keys=" << copy_batch_num
             << ", continuation_token=" << lister.get_continuation_token()
             << ", err=" << cpp_strerror(-ret)
             << ", attempt=" << i;

        if (ret == -ENOENT || ret == -EACCES || i == list_objects_attempts) {
          cerr << std::endl;
          return ret;
        }

        cerr << ", retry in " << list_objects_retry_sleep_seconds << " seconds" << std::endl;
        utime_t retry(list_objects_retry_sleep_seconds, 0);
        retry.sleep();
      }
    }

    /*
    XMLFormatter formatter(true);
    RGWStreamFlusher f(&formatter, cout);
    formatter.output_header();
    listResp->dump_xml(&formatter);
    formatter.output_footer();
    f.flush();
    */
    rgw_bucket src_bucket;
    src_bucket.tenant = tenant;
    src_bucket.name = bucket_name;

    for (const auto &obj : listResp->contents) {
      ldout(store->ctx(), 10) << "copy object=" << obj.key
                              << ", size=" << obj.size
                              << ", bucket=" << bucket_name << dendl;

      rgw_obj src_obj(src_bucket, obj.key);
      rgw_obj dest_obj(dest_bucket, obj.key);

      RGWObjectCtx obj_ctx(store);
      string user_id;
      RGWBucketInfo src_bucket_info;
      std::optional<rgw_placement_rule> dest_placement_rule;
      std::optional<uint64_t> versioned_epoch;
      map<string, bufferlist> attrs;
      std::optional<uint64_t> bytes_transferred;

      int r = store->fetch_remote_obj(obj_ctx,
                                      user_id,
                                      NULL,
                                      DO_BUCKET_COPY_SOURCE_ZONE_ID,
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
        cerr << "ERROR: could not copy object " << obj.key << ", bucket=" << bucket_name
             << ", ret_val=" << r << std::endl;
        if (r == -ENOENT) {
          // possibly due to stale index on the remote bucket
          stats.not_found++;
        } else {
          stats.error++;
        }
      } else {
        stats.ok++;
      }

      utime_t obj_copy;
      obj_copy.set_from_double(g_conf().get_val<double>("rgw_bucket_copy_obj_sleep"));
      if (obj_copy != utime_t()) {
        ldout(store->ctx(), 20) << "copy object sleeping for " << obj_copy << " secconds"
                                << dendl;
        obj_copy.sleep();
      }
    }

    if (!listResp->is_truncated) {
      break;
    }
  }

  // Dump bucket copy stats in the end
  JSONFormatter formatter(true);
  formatter.open_object_section("stats");
  stats.dump(&formatter);
  formatter.close_section();
  formatter.flush(cout);

  return 0;
}
