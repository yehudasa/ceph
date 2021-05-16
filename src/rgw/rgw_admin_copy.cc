// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
#include <iostream>

#include <boost/optional.hpp>

#include "common/dout.h"

#include "rgw_user.h"
#include "rgw_rest_conn.h"
#include "rgw_admin_copy.h"

#define dout_subsys ceph_subsys_rgw

void bucket_copy::S3ListBucketEntry::decode_xml(XMLObj *obj)
{
  key.clear();
  etag.clear();
  size = 0;
  mtime = ceph::real_time();
  owner = Owner();

  RGWXMLDecoder::decode_xml("Key", key, obj, true);
  RGWXMLDecoder::decode_xml("ETag", etag, obj, true);
  RGWXMLDecoder::decode_xml("Size", size, obj, true);

  // The owner field is not present default.
  RGWXMLDecoder::decode_xml("Owner", owner, obj, false);

  string mtime_str;
  RGWXMLDecoder::decode_xml("LastModified", mtime_str, obj, true);
  boost::optional<ceph::real_time> date = ceph::from_iso_8601(mtime_str);
  if (date == boost::none) {
    throw RGWXMLDecoder::err("invalid LastModified value");
  }
  mtime = *date;
}

void bucket_copy::S3ListBucketEntry::dump_xml(Formatter *f) const
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

void bucket_copy::S3ListBucketResp::decode_xml(XMLObj *obj)
{
  common_prefixes.clear();
  contents.clear();
  marker.clear();
  delimiter.clear();
  is_truncated = false;
  max_keys = 0;
  name.clear();
  next_marker.clear();
  prefix.clear();

  // mandatory
  RGWXMLDecoder::decode_xml("Name", name, obj, true);
  RGWXMLDecoder::decode_xml("MaxKeys", max_keys, obj, true);
  RGWXMLDecoder::decode_xml("IsTruncated", is_truncated, obj, true);

  // optional
  RGWXMLDecoder::decode_xml("Marker", marker, obj, false);
  RGWXMLDecoder::decode_xml("NextMarker", next_marker, obj, false);
  RGWXMLDecoder::decode_xml("Delimiter", delimiter, obj, false);
  RGWXMLDecoder::decode_xml("Prefix", prefix, obj, false);
  RGWXMLDecoder::decode_xml("CommonPrefixes", common_prefixes, obj, false);
  // Contents node may not exist when listing with prefix and delimiter.
  RGWXMLDecoder::decode_xml("Contents", contents, obj, false);
}

void bucket_copy::S3ListBucketResp::dump_xml(Formatter *f) const
{
  f->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  f->dump_string("Name", name);
  f->dump_string("Prefix", prefix);
  f->dump_int("MaxKeys", max_keys);
  f->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (!marker.empty()) {
    f->dump_string("Marker", marker);
  }
  if (!next_marker.empty()) {
    f->dump_string("NextMarker", next_marker);
  }
  if (!delimiter.empty()) {
    f->dump_string("Delimiter", delimiter);
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


const std::string& bucket_copy::BucketObjLister::get_next_token() const
{
  if (is_truncated) {
    return next_marker;
  }

  return last_obj_key;
}

int bucket_copy::BucketObjLister::fetch_next(S3ListBucketResp &resp, uint64_t max_keys)
{
  string resource("/" + bucket_name);
  param_vec_t params;
  params.push_back(param_pair_t("max-keys", to_string(max_keys)));
  params.push_back(param_pair_t("fetch-owner", (fetch_owner ? "true" : "false")));
  if (!prefix.empty()) {
    params.push_back(param_pair_t("prefix", prefix));
  }
  if (!delimiter.empty()) {
    params.push_back(param_pair_t("delimiter", delimiter));
  }

  string next_token(get_next_token());
  if (!next_token.empty()) {
    params.push_back(param_pair_t("marker", next_token));
  }

  // RGW internal parameters
  params.push_back(param_pair_t("allow-unordered", (allow_unordered ? "true" : "false")));

  map<string, string> extra_headers;
  bufferlist in, out;

  int r = conn->get_resource(resource, &params, &extra_headers, out, &in);
  if (r < 0) {
    return r;
  }

  RGWXMLParser parser;
  if (!parser.init()) {
    return -EINVAL;
  }

  if (!parser.parse(out.c_str(), out.length(), 1)) {
    ldout(cct, 0) << "ERROR: could not parse list bucket XML response" << dendl;
    return -ERR_MALFORMED_XML;
  }

  try {
    RGWXMLDecoder::decode_xml("ListBucketResult", resp, &parser);
  } catch (RGWXMLDecoder::err &err) {
    ldout(cct, 0) << "ERROR: could not decode XML node ListBucketResult: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  // keep track of current list status in order to list next batch
  is_truncated = resp.is_truncated;
  next_marker = resp.next_marker;

  if (resp.contents.empty()) {
    last_obj_key.clear();
  } else {
    last_obj_key = resp.contents.back().key;
  }

  return 0;
}

int bucket_copy::CopyObjTask::run()
{
  RGWObjectCtx obj_ctx(store);
  rgw_user user_id;
  rgw_obj dest_obj(dest_bucket, obj_key);
  rgw_obj src_obj(src_bucket, obj_key);
  RGWBucketInfo src_bucket_info;
  std::optional<rgw_placement_rule> dest_placement_rule;
  std::optional<uint64_t> versioned_epoch;
  map<string, bufferlist> attrs;
  std::optional<uint64_t> bytes_transferred;

  auto attempts = g_conf().get_val<uint64_t>("rgw_bucket_copy_obj_attempts");
  auto retry_sleep = g_conf().get_val<std::chrono::seconds>("rgw_bucket_copy_obj_retry_sleep");

  int r;
  for (uint i = 1; i <= attempts; i++) {
    ldout(store->ctx(), 5) << "copy remote object " << obj_key
                           << ", bucket=" << src_bucket.name
                           << ", attempt=" << i
                           << dendl;

    r = store->fetch_remote_obj(obj_ctx,
                                user_id,
                                NULL,
                                BUCKET_COPY_SOURCE_ZONE_ID,
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

    stats.count_copy(r, *bytes_transferred);

    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: could not copy remote object " << obj_key
                             << ", bucket=" << src_bucket.name
                             << ", attempt=" << i
                             << ", err=" << cpp_strerror(-r)
                             << dendl;

      if (r == -ENOENT || r == -EACCES || i == attempts) {
        break;
      }

      ldout(store->ctx(), 5) << "copy remote object " << obj_key
                             << ", will retry in " << retry_sleep
                             << dendl;
      std::this_thread::sleep_for(retry_sleep);
    } else {
      break;
    }
  }

  return r;
}

int bucket_copy::copy_remote_bucket(RGWRados *store,
                                    RGWBucketInfo &dest_bucket_info,
                                    const rgw_bucket &dest_bucket,
                                    const string &tenant,
                                    const string &bucket_name,
                                    const string &object_prefix,
                                    const list<string> &endpoints,
                                    const RGWAccessKey &key)
{
  // We need to inject the RGWRESTConn for the remote cluster into the
  // RGWSI_Zone::zone_conn_map, so that RGWRados::fetch_remote_obj can be
  // reused, where it looks for the RGWRESTConn by source_zone when fetching
  // remote objects.
  auto& zone_conn_map = store->svc.zone->get_zone_conn_map();
  map<string, RGWRESTConn *>::const_iterator it = zone_conn_map.find(BUCKET_COPY_SOURCE_ZONE_ID);

  // Though this should never happen, we need to ensure it does not exist in
  // the zone_conn_map before injecting the new RGWRESTConn.
  if (it != zone_conn_map.cend()) {
    ldout(store->ctx(), 0) << "ERROR: source zone " << BUCKET_COPY_SOURCE_ZONE_ID << " already exists"
                           << dendl;
    return -EEXIST;
  }

  // RGWRESTConn needs to remain allocated, because we inject it into
  // zone_conn_map which will be properly destroyed by RGWSI_Zone::shutdown.
  RGWRESTConn *conn = new RGWRESTConn(store->ctx(),
                                      nullptr, // RGWSI_Zone *zone_svc
                                      "", // const string& _remote_id
                                      endpoints,
                                      key);

  zone_conn_map[BUCKET_COPY_SOURCE_ZONE_ID] = conn;

  auto num_threads = g_conf().get_val<uint64_t>("rgw_bucket_copy_obj_threads");

  rgw_bucket src_bucket;
  src_bucket.tenant = tenant;
  src_bucket.name = bucket_name;

  bucket_copy::BucketObjLister lister(store->ctx(), conn, bucket_name, object_prefix);
  bucket_copy::Runner<CopyObjTask> runner(num_threads);
  bucket_copy::Stats stats(g_ceph_context);

  while (true) {
    auto batch_num = g_conf().get_val<uint64_t>("rgw_bucket_copy_list_batch_num");
    auto attempts = g_conf().get_val<uint64_t>("rgw_bucket_copy_list_attempts");
    auto retry_sleep = g_conf().get_val<std::chrono::seconds>("rgw_bucket_copy_list_retry_sleep");

    bucket_copy::S3ListBucketResp listResp;

    for (uint i = 1; i <= attempts; i++) {
      ldout(store->ctx(), 5) << "list remote bucket " << bucket_name
                             << ", max_keys=" << batch_num
                             << ", marker=" << lister.get_next_token()
                             << ", attempt=" << i
                             << dendl;

      auto start = ceph_clock_now();
      int r = lister.fetch_next(listResp, batch_num);
      stats.count_list(r, ceph_clock_now() - start);

      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: could not list remote bucket " << bucket_name
                               << ", max_keys=" << batch_num
                               << ", marker=" << lister.get_next_token()
                               << ", attempt=" << i
                               << ", err=" << cpp_strerror(-r)
                               << dendl;

        if (r == -ENOENT || r == -EACCES || i == attempts) {
          runner.done();
          return r;
        }

        ldout(store->ctx(), 5) << "list remote bucket " << bucket_name
                               << ", will retry in " << retry_sleep
                               << dendl;
        std::this_thread::sleep_for(retry_sleep);
      } else {
        break;
      }
    }

    for (const auto &obj : listResp.contents) {
      runner.submit(CopyObjTask(store,
                                stats,
                                dest_bucket_info,
                                dest_bucket,
                                src_bucket,
                                obj.key));

      // early terminate in case of copy object errors
      int r = runner.status();
      if (r < 0) {
        return r;
      }
    }

    if (!listResp.is_truncated) {
      runner.done();
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
