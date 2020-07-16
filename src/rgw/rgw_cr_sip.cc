#include "rgw_cr_sip.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_sync_info.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int SIProviderCRMgr::GetNextStageCR::operate()
{
  reenter(this) {
    yield call(mgr->get_stages_cr(&stages));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    bool found = (sid.empty()); /* for empty stage id return the first stage */
    for (auto& stage : stages) {
      if (found) {
        *next_sid = stage;
        return set_cr_done();
      }

      if (stage == sid) {
        found = true;
      }
    }

    int ret = (found ? -ENODATA : -ENOENT);
    return set_cr_error(ret);
  }

  return 0;
}

template <class T>
class RGWSafeRetAsyncCR : public RGWCoroutine {
  friend struct Action;

  RGWAsyncRadosProcessor *async_rados;

  T *pret;
  std::function<int(T *)> cb;

  struct Action : public RGWGenericAsyncCR::Action {
    RGWSafeRetAsyncCR *caller;
    T ret;

    Action(RGWSafeRetAsyncCR *_caller) : caller(_caller) {}

    int operate() override {
      return caller->cb(&ret);
    }
  };

  std::shared_ptr<Action> action;

public:
  RGWSafeRetAsyncCR(CephContext *cct,
                    RGWAsyncRadosProcessor *_async_rados,
                    T *_pret,
                    std::function<int(T *)> _cb) : RGWCoroutine(cct),
                               async_rados(_async_rados),
                               pret(_pret),
                               cb(_cb) {}

  int operate() {
    reenter(this) {
      action = make_shared<Action>(this);

      yield call(new RGWGenericAsyncCR(cct, async_rados, action));

      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      /* now it is safe to copy return value */
      *pret = action->ret;

      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine *SIProviderCRMgr_Local::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<std::vector<SIProvider::stage_id_t> >(cct,
                               async_rados,
                               stages,
                               [=](std::vector<SIProvider::stage_id_t> *_stages) {
                                 *_stages = pvd->get_stages();
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::StageInfo>(cct,
                               async_rados,
                               stage_info,
                               [=](SIProvider::StageInfo *_stage_info) {
                                 return pvd->get_stage_info(sid, _stage_info);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::fetch_result>(cct,
                               async_rados,
                               result,
                               [=](SIProvider::fetch_result *_result) {
                                 return pvd->fetch(sid, shard_id, marker, max, _result);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<std::string>(cct,
                               async_rados,
                               marker,
                               [=](std::string *_marker) {
                                 return pvd->get_start_marker(sid, shard_id, _marker);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<std::string>(cct,
                               async_rados,
                               marker,
                               [=](std::string *_marker) {
                                 return pvd->get_cur_state(sid, shard_id, _marker);
                               });
}

struct SIProviderRESTCRs {
  class GetStagesInfoCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;

    string path;
    SIProvider::Info *info;
  public:
    GetStagesInfoCR(SIProviderCRMgr_REST *_mgr,
                    SIProvider::Info *_info) : RGWCoroutine(_mgr->ctx()),
                                               mgr(_mgr),
                                               info(_info) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "info", nullptr },
					  { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
                                         path,
                                         pairs,
                                         info));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    std::vector<SIProvider::stage_id_t> *result;

    SIProvider::Info info;
  public:
    GetStagesCR(SIProviderCRMgr_REST *_mgr,
                std::vector<SIProvider::stage_id_t> *_result) : RGWCoroutine(_mgr->ctx()),
                                                                mgr(_mgr),
                                                                result(_result) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgr, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        result->clear();
        result->reserve(info.stages.size());

        for (auto& sinfo : info.stages) {
          result->push_back(sinfo.sid);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStageInfoCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;

    SIProvider::Info info;
    SIProvider::StageInfo *sinfo;
  public:
    GetStageInfoCR(SIProviderCRMgr_REST *_mgr,
                   const SIProvider::stage_id_t& _sid,
                   SIProvider::StageInfo *_sinfo) : RGWCoroutine(_mgr->ctx()),
                                                    mgr(_mgr),
                                                    sid(_sid),
                                                    sinfo(_sinfo) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgr, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        for (auto& si : info.stages) {
          if (si.sid == sid) {
            *sinfo = si;
            return set_cr_done();
          }
        }

        ldout(mgr->ctx(), 10) << "GetStageInfoCR(): sid not found: provider=" << mgr->remote_provider_name << " sid=" << sid << dendl;

        return set_cr_error(-ENOENT);
      }

      return 0;
    }
  };

  class FetchCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;
    int max;

    string  path;

    bufferlist bl;
    SIProvider::fetch_result *result;

  public:
    FetchCR(SIProviderCRMgr_REST *_mgr,
            const SIProvider::stage_id_t& _sid,
            int _shard_id,
            const string& _marker,
            int _max,
            SIProvider::fetch_result *_result) : RGWCoroutine(_mgr->ctx()),
                                                 mgr(_mgr),
                                                 sid(_sid),
                                                 shard_id(_shard_id),
                                                 marker(_marker),
                                                 max(_max),
                                                 result(_result) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          char max_buf[16];
          snprintf(max_buf, sizeof(max_buf), "%d", max);
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          rgw_http_param_pair pairs[] = { { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
					  { "max" , max_buf },
					  { "marker" , marker.c_str() },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
                                         path,
                                         pairs,
                                         &bl));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }


        JSONParser p;
        if (!p.parse(bl.c_str(), bl.length())) {
          ldout(cct, 0) << "ERROR: failed to parse fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(-EIO);
        }

        auto type_handler = mgr->type_provider->get_type_handler();
        if (!type_handler) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): get_type_provider for sid=" << sid << " is null, likely a bug" << dendl;
          return set_cr_error(-EIO);
        }

        int r = type_handler->decode_json_results(sid, &p, result);
        if (r < 0) {
          ldout(cct, 0) << "ERROR: failed to decode fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(r);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesStatusCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;
    int shard_id;

    string *start_marker;
    string *cur_marker;

    string path;

    struct {
      struct {
        string start;
        string current;

        void decode_json(JSONObj *obj) {
          JSONDecoder::decode_json("start", start, obj);
          JSONDecoder::decode_json("current", current, obj);
        }
      } markers;

      void decode_json(JSONObj *obj) {
        JSONDecoder::decode_json("markers", markers, obj);
      }
    } status;

  public:
    GetStagesStatusCR(SIProviderCRMgr_REST *_mgr,
                      const SIProvider::stage_id_t& _sid,
                      int _shard_id,
                      string *_start_marker,
                      string *_cur_marker) : RGWCoroutine(_mgr->ctx()),
                                              mgr(_mgr),
                                              sid(_sid),
                                              shard_id(_shard_id),
                                              start_marker(_start_marker),
                                              cur_marker(_cur_marker) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "status", nullptr },
					  { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
                                         path,
                                         pairs,
                                         &status));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        if (start_marker) {
          *start_marker = status.markers.start;
        }

        if (cur_marker) {
          *cur_marker = status.markers.current;
        }

        return set_cr_done();
      }

      return 0;
    }
  };
};

RGWCoroutine *SIProviderCRMgr_REST::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  return new SIProviderRESTCRs::GetStagesCR(this, stages);
}

RGWCoroutine *SIProviderCRMgr_REST::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *sinfo)
{
  return new SIProviderRESTCRs::GetStageInfoCR(this, sid, sinfo);
}

RGWCoroutine *SIProviderCRMgr_REST::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  return new SIProviderRESTCRs::FetchCR(this, sid, shard_id,
                                        marker, max, result);
}

RGWCoroutine *SIProviderCRMgr_REST::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  marker, nullptr);
}

RGWCoroutine *SIProviderCRMgr_REST::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  nullptr, marker);
}

class SIPClientCRMgr
{
  friend class InitMarkersCR;

  static constexpr int spawn_window = 16;

  CephContext *cct;
  std::shared_ptr<SIProviderCRMgr> provider;

  std::vector<SIProvider::stage_id_t> stages;
  std::vector<SIProvider::StageInfo> sinfo;

  struct State {
    std::vector<std::string> markers;
    std::map<SIProvider::stage_id_t, std::vector<std::string> > initial_stage_markers;
    SIProvider::StageInfo stage_info;
    int num_complete{0};
    std::vector<bool> done;
  } state;

  int init_stage(const SIProvider::stage_id_t& new_sid, SIProvider::StageInfo& stage_info);

  class InitMarkersCR : public RGWCoroutine {
    SIPClientCRMgr *mgr;
    SIProviderCRMgr *provider;

    std::vector<SIProvider::stage_id_t>& stages;
    std::vector<SIProvider::StageInfo>& sinfo;

    SIProvider::StageInfo prev;
    int i;
    int shard_id;
    bool all_history;
    std::vector<std::string> *stage_markers;
    RGWCoroutine *cr;
  public:
    InitMarkersCR(SIPClientCRMgr *_mgr) : RGWCoroutine(mgr->cct),
                                          mgr(_mgr),
                                          provider(mgr->provider.get()),
                                          stages(mgr->stages),
                                          sinfo(mgr->sinfo) {}

    int operate() override;
  };

  class FetchCR : public RGWCoroutine {
    SIPClientCRMgr *mgr;
    SIProviderCRMgr *provider;

    SIPClientCRMgr::State& state;

    int shard_id;
    int max;
    SIProvider::fetch_result *result;

  public:
    FetchCR(SIPClientCRMgr *_mgr,
            int _shard_id,
            int _max,
            SIProvider::fetch_result *_result) : RGWCoroutine(mgr->cct),
                                                 mgr(_mgr),
                                                 provider(mgr->provider.get()),
                                                 state(mgr->state),
                                                 shard_id(_shard_id),
                                                 max(_max),
                                                 result(_result) {}

    int operate() override;
  };

  class PromoteStageCR : public RGWCoroutine {
    SIPClientCRMgr *mgr;
    SIProviderCRMgr *provider;

    std::vector<SIProvider::stage_id_t>& stages;
    std::vector<SIProvider::StageInfo>& sinfo;
    SIPClientCRMgr::State& state;

    int *new_num_shards;

    bool found{false};
    int i;

  public:
    PromoteStageCR(SIPClientCRMgr *_mgr,
                   int *_new_num_shards) : RGWCoroutine(mgr->cct),
                                           mgr(_mgr),
                                           provider(mgr->provider.get()),
                                           stages(mgr->stages),
                                           sinfo(mgr->sinfo),
                                           state(mgr->state),
                                           new_num_shards(_new_num_shards) {}

    int operate() override;
  };

public:
  SIPClientCRMgr(CephContext *_cct,
                 std::shared_ptr<SIProviderCRMgr> _provider) : cct(_cct),
                                                               provider(_provider) {}
  int stage_num_shards() const {
    return state.stage_info.num_shards;
  }

  bool is_shard_done(int shard_id) const {
    return (shard_id < stage_num_shards() &&
            state.done[shard_id]);
  }

  bool stage_complete() const {
    return (state.num_complete == stage_num_shards());
  }

  RGWCoroutine *init_markers_cr() {
    return new InitMarkersCR(this);
  }

  RGWCoroutine *fetch_cr(int shard_id, int max, SIProvider::fetch_result *result) {
    return new FetchCR(this, shard_id, max, result);
  }

  RGWCoroutine *promote_stage_cr(int *new_num_shards) {
    return new PromoteStageCR(this, new_num_shards);
  }
};


int SIPClientCRMgr::InitMarkersCR::operate()
{
  reenter(this) {
    yield call(provider->get_stages_cr(&stages));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    if (stages.empty()) {
      return set_cr_done();
    }

    sinfo.resize(stages.size());

    for (i = 0; i < (int)stages.size(); ++i) {
      yield_spawn_window(provider->get_stage_info_cr(stages[i], &sinfo[i]),
                         mgr->spawn_window,
                         [&](int stack_id, int ret) {
                           ldout(mgr->cct, 0) << "failed to get sync stage info for sid=" << stages[i] << ": ret=" << ret << dendl;
                           return ret;
                         });
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
    }

    drain_all_cb([&](int stack_id, int ret) {
                   ldout(mgr->cct, 10) << "failed to get sync stage info: ret=" << ret << dendl;
                   return ret;
                 });

    for (i = 0; i < (int)stages.size(); ++i) {
      all_history = (prev.type != SIProvider::StageType::FULL ||
                     sinfo[i].type != SIProvider::StageType::INC);
      stage_markers = &(mgr->state.initial_stage_markers[sinfo[i].sid]);
      stage_markers->resize(sinfo[i].num_shards);
      for (shard_id = 0; shard_id < sinfo[i].num_shards; ++shard_id) {
        cr = (!all_history ? provider->get_cur_state_cr(stages[i], shard_id, &(*stage_markers)[shard_id]) : 
              provider->get_start_marker_cr(stages[i], shard_id, &(*stage_markers)[shard_id]));
        yield_spawn_window(cr,
                           mgr->spawn_window,
                           [&](int stack_id, int ret) {
                             ldout(mgr->cct, 0) << "failed to get marker info: ret=" << ret << dendl;
                             return ret;
                           });
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
      }

      drain_all_cb([&](int stack_id, int ret) {
                     ldout(mgr->cct, 10) << "failed to get stage marker info: ret=" << ret << dendl;
                     return ret;
                   });

      prev = sinfo[i];
    }

    retcode = mgr->init_stage(stages[0], sinfo[0]);
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    return set_cr_done();
  }

  return 0;
}

int SIPClientCRMgr::init_stage(const SIProvider::stage_id_t& new_sid, SIProvider::StageInfo& stage_info)
{
  auto& markers = state.markers;
  auto& done = state.done;
  auto& stage_markers = state.initial_stage_markers;

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
  state.stage_info = stage_info;

  return 0;
}

int SIPClientCRMgr::FetchCR::operate()
{
  reenter(this) {
    if (shard_id > state.stage_info.num_shards) {
      return -ERANGE;
    }

    yield call(provider->fetch_cr(state.stage_info.sid, shard_id, state.markers[shard_id], max, result));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    if (!result->entries.empty()) {
      state.markers[shard_id] = result->entries.back().key;
    }

    auto& done = state.done;

    if (result->done && !done[shard_id]) {
      ++state.num_complete;
      done[shard_id] = result->done;
    }

    return set_cr_done();
  }

  return 0;
}

int SIPClientCRMgr::PromoteStageCR::operate()
{
  reenter(this) {
    for (i = 0; i < (int)stages.size() - 1; ++i) {
      if (stages[i] == state.stage_info.sid) {
        found = true;
        ++i;
        break;
      }
    }

    if (!found) {
      i = sinfo.size();
      sinfo.resize(i + 1);
      call(provider->get_next_stage_cr(state.stage_info.sid, &stages[i]));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      yield provider->get_stage_info_cr(stages[i], &sinfo[i]);
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
    }

    retcode = mgr->init_stage(stages[i], sinfo[i]);
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    if (new_num_shards) {
      *new_num_shards = mgr->stage_num_shards();
    }

    return set_cr_done();
  }

  return 0;
}

