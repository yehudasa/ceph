
#include "rgw_cr_sync_group.h"

#define dout_subsys ceph_subsys_rgw

template<>
int RGWSyncShardGroupInitCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int r = store->svc()->cls->sync_shard_group.init_group(params.key,
                                                         params.group_id,
                                                         params.num_shards,
                                                         params.exclusive,
                                                         null_yield);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: sync shard group init failed: obj=" << params.key << " group_id=" << params.group_id << " exclusive=" << params.exclusive << " r=" << r << dendl;
    return r;
  }

  return 0;
}

template<>
int RGWSyncShardGroupUpdateCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int r = store->svc()->cls->sync_shard_group.update_completion(params.key,
                                                                params.group_id,
                                                                params.entries,
                                                                &result->all_complete,
                                                                null_yield);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: sync shard group update failed: obj=" << params.key << " group_id=" << params.group_id << " r=" << r << dendl;
    return  r;
  }

  return 0;
}

template<>
int RGWSyncShardGroupGetInfoCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int r = store->svc()->cls->sync_shard_group.get_info(params.key,
                                                       params.group_id,
                                                       &result->result,
                                                       null_yield);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: sync shard group get_info failed: obj=" << params.key << " group_id=" << params.group_id << " r=" << r << dendl;
    return  r;
  }

  return 0;
}

template<>
int RGWSyncShardGroupListCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int r = store->svc()->cls->sync_shard_group.list_group(params.key,
                                                         params.group_id,
                                                         params.marker,
                                                         params.max_entries,
                                                         &result->result,
                                                         &result->more,
                                                         null_yield);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: sync shard group get_info failed: obj=" << params.key << " group_id=" << params.group_id <<
      " marker=" << (params.marker ? (int64_t)(*params.marker) : -1) << " max_entries=" << params.max_entries << " r=" << r << dendl;
    return  r;
  }

  return 0;
}

template<>
int RGWSyncShardGroupPurgeCR::Request::_send_request()
{
  CephContext *cct = store->ctx();

  int r = store->svc()->cls->sync_shard_group.purge_group(params.key,
                                                          params.group_id,
                                                          null_yield);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: sync shard group purge failed: obj=" << params.key << " group_id=" << params.group_id << dendl;
    return r;
  }

  return 0;
}

