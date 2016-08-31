// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_http_errors.h"
#include "rgw_rados.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/strtol.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

int RGWRESTSimpleRequest::get_status()
{
  int retcode = get_req_retcode();
  if (retcode < 0) {
    return retcode;
  }
  return status;
}

int RGWRESTSimpleRequest::handle_header(const string& name, const string& val) 
{
  if (name == "CONTENT_LENGTH") {
    string err;
    long len = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting content length (" << val << ") to int " << dendl;
      return -EINVAL;
    }

    max_response = len;
  }

  return 0;
}

int RGWRESTSimpleRequest::receive_header(void *ptr, size_t len)
{
  char line[len + 1];

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  ldout(cct, 10) << "receive_http_header" << dendl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      ldout(cct, 10) << "received header:" << line << dendl;
      // TODO: fill whatever data required here
      char *l = line;
      char *tok = strsep(&l, " \t:");
      if (tok && l) {
        while (*l == ' ')
          l++;
 
        if (strcmp(tok, "HTTP") == 0 || strncmp(tok, "HTTP/", 5) == 0) {
          http_status = atoi(l);
          if (http_status == 100) /* 100-continue response */
            continue;
          status = rgw_http_error_to_errno(http_status);
        } else {
          /* convert header field name to upper case  */
          char *src = tok;
          char buf[len + 1];
          size_t i;
          for (i = 0; i < len && *src; ++i, ++src) {
            switch (*src) {
              case '-':
                buf[i] = '_';
                break;
              default:
                buf[i] = toupper(*src);
            }
          }
          buf[i] = '\0';
          out_headers[buf] = l;
          int r = handle_header(buf, l);
          if (r < 0)
            return r;
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return 0;
}

static void get_new_date_str(CephContext *cct, string& date_str)
{
  utime_t tm = ceph_clock_now(cct);
  stringstream s;
  tm.asctime(s);
  date_str = s.str();
}

int RGWRESTSimpleRequest::execute(RGWAccessKey& key, const char *method, const char *resource)
{
  string new_url = url;
  string new_resource = resource;

  if (new_url[new_url.size() - 1] == '/' && resource[0] == '/') {
    new_url = new_url.substr(0, new_url.size() - 1);
  } else if (resource[0] != '/') {
    new_resource = "/";
    new_resource.append(resource);
  }
  new_url.append(new_resource);

  string date_str;
  get_new_date_str(cct, date_str);
  headers.push_back(pair<string, string>("HTTP_DATE", date_str));

  string canonical_header;
  map<string, string> meta_map;
  map<string, string> sub_resources;
  rgw_create_s3_canonical_header(method, NULL, NULL, date_str.c_str(),
                            meta_map, new_url.c_str(), sub_resources,
                            canonical_header);

  string digest;
  int ret = rgw_get_s3_header_digest(canonical_header, key.key, digest);
  if (ret < 0) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;

  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;

  headers.push_back(pair<string, string>("AUTHORIZATION", auth_hdr));
  int r = process(method, new_url.c_str());
  if (r < 0)
    return r;

  return status;
}

int RGWRESTSimpleRequest::send_data(void *ptr, size_t len)
{
  if (!send_iter)
    return 0;

  if (len > send_iter->get_remaining())
    len = send_iter->get_remaining();

  send_iter->copy(len, (char *)ptr);

  return len;
}

int RGWRESTSimpleRequest::receive_data(void *ptr, size_t len)
{
  size_t cp_len, left_len;

  left_len = max_response > response.length() ? (max_response - response.length()) : 0;
  if (left_len == 0)
    return 0; /* don't read extra data */

  cp_len = (len > left_len) ? left_len : len;
  bufferptr p((char *)ptr, cp_len);

  response.append(p);

  return 0;

}

void RGWRESTSimpleRequest::append_param(string& dest, const string& name, const string& val)
{
  if (dest.empty()) {
    dest.append("?");
  } else {
    dest.append("&");
  }
  string url_name;
  url_encode(name, url_name);
  dest.append(url_name);

  if (!val.empty()) {
    string url_val;
    url_encode(val, url_val);
    dest.append("=");
    dest.append(url_val);
  }
}

void RGWRESTSimpleRequest::get_params_str(map<string, string>& extra_args, string& dest)
{
  map<string, string>::iterator miter;
  for (miter = extra_args.begin(); miter != extra_args.end(); ++miter) {
    append_param(dest, miter->first, miter->second);
  }
  param_vec_t::iterator iter;
  for (iter = params.begin(); iter != params.end(); ++iter) {
    append_param(dest, iter->first, iter->second);
  }
}

int RGWRESTSimpleRequest::sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info)
{
  /* don't sign if no key is provided */
  if (key.key.empty()) {
    return 0;
  }

  map<string, string, ltstr_nocase>& m = env.get_map();

  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    map<string, string>::iterator i;
    for (i = m.begin(); i != m.end(); ++i) {
      ldout(cct, 20) << "> " << i->first << " -> " << i->second << dendl;
    }
  }

  string canonical_header;
  if (!rgw_create_s3_canonical_header(info, NULL, canonical_header, false)) {
    ldout(cct, 0) << "failed to create canonical s3 header" << dendl;
    return -EINVAL;
  }

  ldout(cct, 10) << "generated canonical header: " << canonical_header << dendl;

  string digest;
  int ret = rgw_get_s3_header_digest(canonical_header, key.key, digest);
  if (ret < 0) {
    return ret;
  }

  string auth_hdr = "AWS " + key.id + ":" + digest;
  ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;
  
  m["AUTHORIZATION"] = auth_hdr;

  return 0;
}

int RGWRESTSimpleRequest::forward_request(RGWAccessKey& key, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{

  string date_str;
  get_new_date_str(cct, date_str);

  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  new_info.rebuild_from(info);

  new_env.set("HTTP_DATE", date_str.c_str());

  int ret = sign_request(key, new_env, new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  map<string, string, ltstr_nocase>& m = new_env.get_map();
  map<string, string>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    headers.push_back(pair<string, string>(iter->first, iter->second));
  }

  map<string, string>& meta_map = new_info.x_meta_map;
  for (iter = meta_map.begin(); iter != meta_map.end(); ++iter) {
    headers.push_back(pair<string, string>(iter->first, iter->second));
  }

  string params_str;
  get_params_str(info.args.get_params(), params_str);

  string new_url = url;
  string& resource = new_info.request_uri;
  string new_resource = resource;
  if (new_url[new_url.size() - 1] == '/' && resource[0] == '/') {
    new_url = new_url.substr(0, new_url.size() - 1);
  } else if (resource[0] != '/') {
    new_resource = "/";
    new_resource.append(resource);
  }
  new_url.append(new_resource + params_str);

  bufferlist::iterator bliter;

  if (inbl) {
    bliter = inbl->begin();
    send_iter = &bliter;

    set_send_length(inbl->length());
  }

  int r = process(new_info.method, new_url.c_str());
  if (r < 0){
    if (r == -EINVAL){
      // curl_easy has errored, generally means the service is not available
      r = -ERR_SERVICE_UNAVAILABLE;
    }
    return r;
  }

  response.append((char)0); /* NULL terminate response */

  if (outbl) {
    outbl->claim(response);
  }

  return status;
}

void set_str_from_headers(map<string, string>& out_headers, const string& header_name, string& str)
{
  map<string, string>::iterator iter = out_headers.find(header_name);
  if (iter != out_headers.end()) {
    str = iter->second;
  } else {
    str.clear();
  }
}

static int parse_rgwx_mtime(CephContext *cct, const string& s, ceph::real_time *rt)
{
  string err;
  vector<string> vec;

  get_str_vec(s, ".", vec);

  if (vec.empty()) {
    return -EINVAL;
  }

  long secs = strict_strtol(vec[0].c_str(), 10, &err);
  long nsecs = 0;
  if (!err.empty()) {
    ldout(cct, 0) << "ERROR: failed converting mtime (" << s << ") to real_time " << dendl;
    return -EINVAL;
  }

  if (vec.size() > 1) {
    nsecs = strict_strtol(vec[1].c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting mtime (" << s << ") to real_time " << dendl;
      return -EINVAL;
    }
  }

  *rt = utime_t(secs, nsecs).to_real_time();

  return 0;
}

int RGWRESTStreamRWRequest::get_obj(RGWAccessKey& key, map<string, string>& extra_headers, rgw_obj& obj, RGWHTTPManager *mgr)
{
  string urlsafe_bucket, urlsafe_object;
  url_encode(obj.bucket.get_key(':', 0), urlsafe_bucket);
  url_encode(obj.get_orig_obj(), urlsafe_object);
  string resource = urlsafe_bucket + "/" + urlsafe_object;

  return get_resource(key, extra_headers, resource, mgr);
}

int RGWRESTStreamRWRequest::get_resource(RGWAccessKey& key, map<string, string>& extra_headers, const string& resource, RGWHTTPManager *mgr)
{
  string new_url = url;
  if (new_url[new_url.size() - 1] != '/')
    new_url.append("/");

  string date_str;
  get_new_date_str(cct, date_str);

  RGWEnv new_env;
  req_info new_info(cct, &new_env);
  
  string params_str;
  map<string, string>& args = new_info.args.get_params();
  get_params_str(args, params_str);

  /* merge params with extra args so that we can sign correctly */
  for (param_vec_t::iterator iter = params.begin(); iter != params.end(); ++iter) {
    new_info.args.append(iter->first, iter->second);
  }

  string new_resource;
  if (resource[0] == '/') {
    new_resource = resource.substr(1);
  } else {
    new_resource = resource;
  }

  new_url.append(new_resource + params_str);

  new_env.set("HTTP_DATE", date_str.c_str());

  for (map<string, string>::iterator iter = extra_headers.begin();
       iter != extra_headers.end(); ++iter) {
    new_env.set(iter->first.c_str(), iter->second.c_str());
  }

  new_info.method = method;

  new_info.script_uri = "/";
  new_info.script_uri.append(new_resource);
  new_info.request_uri = new_info.script_uri;

  new_info.init_meta_info(NULL);

  int ret = sign_request(key, new_env, new_info);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to sign request" << dendl;
    return ret;
  }

  map<string, string, ltstr_nocase>& m = new_env.get_map();
  map<string, string>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    headers.push_back(pair<string, string>(iter->first, iter->second));
  }

  RGWHTTPManager *pmanager = &http_manager;
  if (mgr) {
    pmanager = mgr;
  } else {
    int ret = http_manager.start();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: http_manager.start() returned ret=" << ret << dendl;
      return ret;
    }
  }

  int r = pmanager->add_request(this, new_info.method, new_url.c_str());
  if (r < 0)
    return r;

  if (!mgr) {
    http_manager.stop();
  }

  return 0;
}

int RGWRESTStreamRWRequest::complete(string& etag, real_time *mtime, uint64_t *psize, map<string, string>& attrs)
{
  set_str_from_headers(out_headers, "ETAG", etag);
  if (status >= 0) {
    if (mtime) {
      string mtime_str;
      set_str_from_headers(out_headers, "RGWX_MTIME", mtime_str);
      if (!mtime_str.empty()) {
        int ret = parse_rgwx_mtime(cct, mtime_str, mtime);
        if (ret < 0) {
          return ret;
        }
      } else {
        *mtime = real_time();
      }
    }
    if (psize) {
      string size_str;
      set_str_from_headers(out_headers, "RGWX_OBJECT_SIZE", size_str);
      string err;
      *psize = strict_strtoll(size_str.c_str(), 10, &err);
      if (!err.empty()) {
        ldout(cct, 0) << "ERROR: failed parsing embedded metadata object size (" << size_str << ") to int " << dendl;
        return -EIO;
      }
    }
  }

  map<string, string>::iterator iter;
  for (iter = out_headers.begin(); iter != out_headers.end(); ++iter) {
    const string& attr_name = iter->first;
    if (attr_name.compare(0, sizeof(RGW_HTTP_RGWX_ATTR_PREFIX) - 1, RGW_HTTP_RGWX_ATTR_PREFIX) == 0) {
      string name = attr_name.substr(sizeof(RGW_HTTP_RGWX_ATTR_PREFIX) - 1);
      const char *src = name.c_str();
      char buf[name.size() + 1];
      char *dest = buf;
      for (; *src; ++src, ++dest) {
        switch(*src) {
          case '_':
            *dest = '-';
            break;
          default:
            *dest = tolower(*src);
        }
      }
      *dest = '\0';
      attrs[buf] = iter->second;
    }
  }
  return status;
}

int RGWRESTStreamRWRequest::handle_header(const string& name, const string& val)
{
  if (name == "RGWX_EMBEDDED_METADATA_LEN") {
    string err;
    long len = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed converting embedded metadata len (" << val << ") to int " << dendl;
      return -EINVAL;
    }

    cb->set_extra_data_len(len);
  }
  return 0;
}

int RGWRESTStreamRWRequest::receive_data(void *ptr, size_t len)
{
  bufferptr bp((const char *)ptr, len);
  bufferlist bl;
  bl.append(bp);
  int ret = cb->handle_data(bl, ofs, len);
  if (ret < 0)
    return ret;
  ofs += len;
  return len;
}

int RGWRESTStreamRWRequest::send_data(void *ptr, size_t len)
{
  if (outbl.length() == 0) {
    return 0;
  }

  uint64_t send_size = min(len, (size_t)(outbl.length() - write_ofs));
  if (send_size > 0) {
    memcpy(ptr, outbl.c_str() + write_ofs, send_size);
    write_ofs += send_size;
  }
  return send_size;
}

class StreamIntoBufferlist : public RGWGetDataCB {
  bufferlist& bl;
public:
  StreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, off_t bl_ofs, off_t bl_len) {
    bl.claim_append(inbl);
    return bl_len;
  }
};

