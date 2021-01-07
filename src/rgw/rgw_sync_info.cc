#include "rgw_sync_info.h"

#include "common/dout.h"
#include "common/ceph_json.h"


#define dout_subsys ceph_subsys_rgw

static inline string stage_type_to_str(SIProvider::StageType st)
{
  switch  (st) {
    case SIProvider::StageType::FULL:
      return "full";
    case SIProvider::StageType::INC:
      return "inc";
    default:
      return "unknown";
  }
}

static inline SIProvider::StageType stage_type_from_str(const string& s)
{
  if (s == "full") {
    return SIProvider::StageType::FULL;
  }
  if (s == "inc") {
    return SIProvider::StageType::INC;
  }

  return SIProvider::StageType::UNKNOWN;
}


void SIProvider::StageInfo::dump(Formatter *f) const
{
  encode_json("sid", sid, f);
  encode_json("next_sid", next_sid, f);
  encode_json("type", stage_type_to_str(type), f);
  encode_json("num_shards", num_shards, f);
}

void SIProvider::StageInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("sid", sid, obj);
  JSONDecoder::decode_json("next_sid", next_sid, obj);
  string type_str;
  JSONDecoder::decode_json("type", type_str, obj);
  type = stage_type_from_str(type_str);
  JSONDecoder::decode_json("num_shards", num_shards, obj);
}

void SIProvider::Info::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("first_stage", first_stage, f);
  encode_json("last_stage", last_stage, f);
  encode_json("stages", stages, f);
}

void SIProvider::Info::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("first_stage", first_stage, obj);
  JSONDecoder::decode_json("last_stage", last_stage, obj);
  JSONDecoder::decode_json("stages", stages, obj);
}

SIProvider::Info SIProviderCommon::get_info()
{
  vector<SIProvider::StageInfo> stages;

  for (auto& sid : get_stages()) {
    SIProvider::StageInfo si;
    int r = get_stage_info(sid, &si);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to retrieve stage info for sip=" << get_name() << ", sid=" << sid << ": r=" << r << dendl;
      /* continuing */
    }
    stages.push_back(si);
  }

  return { get_name(),
           get_first_stage(),
           get_last_stage(),
           stages };
}

int SIProvider_SingleStage::fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_fetch(shard_id, marker, max, result);
}

int SIProvider_SingleStage::get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_start_marker(shard_id, marker);
}

int SIProvider_SingleStage::get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_cur_state(shard_id, marker);
}

int SIProvider_SingleStage::trim(const stage_id_t& sid, int shard_id, const std::string& marker)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_trim(shard_id, marker);
}

SIProvider_Container::SIProvider_Container(CephContext *_cct,
                                           const std::string& _name,
                                           std::vector<SIProviderRef>& _providers) : SIProviderCommon(_cct, _name),
                                                                                     providers(_providers)
{
  std::map<std::string, int> pcount;
  int i = 0;

  for (auto& p : providers) {
    const auto& name = p->get_name();

    int count = pcount[name]++;

    char buf[name.size() + 32 ];
    snprintf(buf, sizeof(buf), "%s/%d", name.c_str(), count);

    providers_index[buf] = i++;
    pids.push_back(buf);
  }
}

bool SIProvider_Container::decode_sid(const stage_id_t& sid,
                                      SIProviderRef *provider,
                                      SIProvider::stage_id_t *provider_sid,
                                      int *index) const
{
  auto pos = sid.find(':');
  if (pos == std::string::npos) {
    return false;
  }

  auto pid = sid.substr(0, pos);
  auto piter = providers_index.find(pid);
  if (piter == providers_index.end()) {
    return false;
  }

  if (index) {
    *index = piter->second;
  }

  *provider = providers[piter->second];
  *provider_sid = sid.substr(pos + 1);

  return true;
}

SIProvider::stage_id_t SIProvider_Container::encode_sid(const std::string& pid,
                                                        const SIProvider::stage_id_t& provider_sid) const
{
  return pid + ":" + provider_sid;
}

SIProvider::stage_id_t SIProvider_Container::get_first_stage()
{
  if (pids.empty()) {
    return stage_id_t();
  }
  return encode_sid(pids[0], providers[0]->get_first_stage());
}

SIProvider::stage_id_t SIProvider_Container::get_last_stage()
{
  if (pids.empty()) {
    return stage_id_t();
  }
  auto i = pids.size() - 1;
  return encode_sid(pids[i], providers[i]->get_last_stage());
}

int SIProvider_Container::get_next_stage(const stage_id_t& sid, stage_id_t *next_sid)
{
  if (pids.empty()) {
    ldout(cct, 20) << "NOTICE: " << __func__ << "() called by pids is empty" << dendl;
    return -ENOENT;
  }

  SIProviderRef provider;
  stage_id_t psid;
  int i;

  if (!decode_sid(sid,  &provider, &psid, &i)) {
    ldout(cct, 0) << "ERROR: " << __func__ << "() psid=" << psid << " failed to decode sid (sid=" << sid << ")" << dendl;
    return -EINVAL;
  }

  SIProvider::stage_id_t next_psid;
  int r = provider->get_next_stage(psid, &next_psid);
  if (r < 0) {
    if (r != -ENOENT) {
      ldout(cct, 0) << "ERROR: " << __func__ << "() psid=" << psid << " returned r=" << r << dendl;
      return r;
    }
    if (++i == (int)providers.size()) {
      ldout(cct, 20) << __func__ << "() reached the last provider" << dendl;
      return -ENOENT;
    }
    provider = providers[i];
    next_psid = provider->get_first_stage();
  }

  *next_sid = encode_sid(pids[i], next_psid);
  return 0;
}

std::vector<SIProvider::stage_id_t> SIProvider_Container::get_stages()
{
  std::vector<stage_id_t> result;

  int i = 0;

  for (auto& provider : providers) {
    auto& pid = pids[i++];

    std::vector<std::string> stages = provider->get_stages();

    for (auto& psid : stages) {
      result.push_back(encode_sid(pid, psid));
    }
  }

  return result;
}

int SIProvider_Container::get_stage_info(const stage_id_t& sid, StageInfo *sinfo)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  int r = provider->get_stage_info(psid, sinfo);
  if (r < 0) {
    return r;
  }

  sinfo->sid = sid;

  stage_id_t next_stage;
  r = get_next_stage(sid, &next_stage);
  if (r >= 0) {
    sinfo->next_sid = next_stage;
  } else {
    sinfo->next_sid.reset();
  }

  return 0;
}

int SIProvider_Container::fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->fetch(psid, shard_id, marker, max, result);
}

int SIProvider_Container::get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_start_marker(psid, shard_id, marker);
}

int SIProvider_Container::get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_cur_state(psid, shard_id, marker);
}

int SIProvider_Container::handle_entry(const stage_id_t& sid,
                                       Entry& entry,
                                       std::function<int(EntryInfoBase&)> f)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->handle_entry(psid, entry, f);
}

int SIProvider_Container::decode_json_results(const stage_id_t& sid,
                                              JSONObj *obj,
                                              SIProvider::fetch_result *result)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->decode_json_results(psid, obj, result);
}

int SIProvider_Container::trim(const stage_id_t& sid, int shard_id, const std::string& marker)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->trim(psid, shard_id, marker);
}

int SIProviderClient::init_markers()
{
  auto stages = provider->get_stages();

  if (stages.empty()) {
    return 0;
  }

  SIProvider::StageInfo prev;

  for (auto& sid : stages) {
    SIProvider::StageInfo sinfo;
    int r = provider->get_stage_info(sid, &sinfo);
    if (r < 0) {
      return r;
    }
    bool all_history = (prev.type != SIProvider::StageType::FULL ||
                        sinfo.type != SIProvider::StageType::INC);
    auto& stage_markers = state.initial_stage_markers[sinfo.sid];
    stage_markers.reserve(sinfo.num_shards);
    for (int i = 0; i < sinfo.num_shards; ++i) {
      std::string marker;
      int r = (!all_history ? provider->get_cur_state(sid, i, &marker) : 
                              provider->get_start_marker(sid, i, &marker));
      if (r < 0) {
        return r;
      }
      stage_markers.push_back(marker);
    }
  }

  init_stage(provider->get_first_stage());

  return 0;
}

int SIProviderClient::init_stage(const stage_id_t& new_sid)
{
  auto& stage_info = state.stage_info;
  auto& markers = state.markers;
  auto& done = state.done;
  auto& stage_markers = state.initial_stage_markers;

  int r = provider->get_stage_info(new_sid, &stage_info);
  if (r < 0) {
    return r;
  }

  auto iter = stage_markers.find(stage_info.sid);
  if (iter != stage_markers.end()) {
    markers = std::move(iter->second);
    stage_markers.erase(iter);
  } else {
    markers.resize(stage_info.num_shards);
    markers.clear();
  }

  done.resize(stage_info.num_shards);
  done.clear();

  state.num_complete = 0;
  return 0;
}


int SIProviderClient::fetch(int shard_id, int max, SIProvider::fetch_result *result) {
  auto& stage_info = state.stage_info;
  auto& markers = state.markers;

  if (shard_id > stage_info.num_shards) {
    return -ERANGE;
  }

  int r = provider->fetch(stage_info.sid, shard_id, markers[shard_id], max, result);
  if (r < 0) {
    return r;
  }

  if (!result->entries.empty()) {
    markers[shard_id] = result->entries.back().key;
  }

  auto& done = state.done;

  if (result->done && !done[shard_id]) {
    ++state.num_complete;
    done[shard_id] = result->done;
  }

  return 0;
}

int SIProviderClient::promote_stage(int *new_num_shards)
{
  stage_id_t next_sid;

  int r = provider->get_next_stage(state.stage_info.sid, &next_sid);
  if (r < 0) {
    return r;
  }

  r = init_stage(next_sid);
  if (r < 0) {
    return r;
  }

  if (new_num_shards) {
    *new_num_shards = stage_num_shards();
  }

  return 0;
}

