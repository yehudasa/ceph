
#include "common/debug.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "rgw_sip_data.h"
#include "rgw_metadata.h"
#include "rgw_b64.h"

#include "services/svc_datalog_rados.h"

#define dout_subsys ceph_subsys_rgw

void siprovider_data_info::dump(Formatter *f) const
{
  encode_json("id", id, f);
}

int SIProvider_DataFull::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id > 0) {
    return -ERANGE;
  }

  string section = "bucket.instance";

  void *handle;

  result->done = false;
  result->more = true;

  auto m = rgw::from_base64(marker);

  int ret = meta.mgr->list_keys_init(section, m, &handle);
  if (ret < 0) {
    lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
    return ret;
  }

  while (max > 0) {
    std::list<RGWMetadataHandler::KeyInfo> entries;
    bool truncated;

    ret = meta.mgr->list_keys_next(handle, max, entries,
                                   &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
      return ret;
    }

    if (!entries.empty()) {
      max -= entries.size();

      m = entries.back().marker;

      for (auto& k : entries) {
        auto e = create_entry(k.key, rgw::to_base64(k.marker));
        result->entries.push_back(e);
      }
    }

    if (!truncated) {
      result->done = true;
      result->more = false;
      break;
    }
  }


  return 0;
}

#if 0
SIProvider_DataInc::SIProvider_DataInc(CephContext *_cct,
				       RGWSI_MDLog *_mdlog,
				       const string& _period_id) : SIProvider_SingleStage(_cct,
                                                                                          "data.inc",
                                                                                          SIProvider::StageType::INC,
                                                                                          _cct->_conf->rgw_md_log_max_shards),
                                                                   mdlog(_mdlog),
                                                                   period_id(_period_id) {}

int SIProvider_DataInc::init()
{
  meta_log = mdlog->get_log(period_id);
  return 0;
}

int SIProvider_DataInc::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id >= stage_info.num_shards) {
    return -ERANGE;
  }

  utime_t start_time;
  utime_t end_time;

  void *handle;

  meta_log->init_list_entries(shard_id, start_time.to_real_time(), end_time.to_real_time(), marker, &handle);
  bool truncated;
  do {
    list<cls_log_entry> entries;
    int ret = meta_log->list_entries(handle, max, entries, NULL, &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: meta_log->list_entries() failed: ret=" << ret << dendl;
      return -ret;
    }

    max -= entries.size();

    for (auto& entry : entries) {
      siprovider_meta_info meta_info(entry.section, entry.name);

      SIProvider::Entry e;
      e.key = entry.id;
      meta_info.encode(e.data);
      result->entries.push_back(e);
    }
  } while (truncated && max > 0);

  result->done = false; /* FIXME */
  result->more = truncated;

  meta_log->complete_list_entries(handle);

  return 0;
}


int SIProvider_DataInc::do_get_start_marker(int shard_id, std::string *marker) const
{
  marker->clear();
  return 0;
}

int SIProvider_DataInc::do_get_cur_state(int shard_id, std::string *marker) const
{
#warning FIXME
  return 0;
}

int SIProvider_DataInc::do_trim(int shard_id, const std::string& marker)
{
  utime_t start_time, end_time;
  int ret;
  // trim until -ENODATA
  do {
    ret = meta_log->trim(shard_id, start_time.to_real_time(),
                         end_time.to_real_time(), string(), marker);
  } while (ret == 0);
  if (ret < 0 && ret != -ENODATA) {
    ldout(cct, 20) << "ERROR: meta_log->trim(): returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}
#endif
