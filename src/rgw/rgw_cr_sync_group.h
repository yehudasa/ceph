// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_cr_rados.h"

struct rgw_sync_shard_group_init_params {
  rgw_raw_obj key;
  string group_id;
  int num_shards{0};
  bool exclusive{false};
};

using RGWSyncShardGroupInitCR = RGWSimpleWriteOnlyAsyncCR<rgw_sync_shard_group_init_params>;

struct rgw_sync_shard_group_update_params {
  rgw_raw_obj key;
  string group_id;
  std::vector<std::pair<uint64_t, bool> > entries;
};

struct rgw_sync_shard_group_update_result {
  bool all_complete{false};
};

using RGWSyncShardGroupUpdateCR = RGWSimpleAsyncCR<rgw_sync_shard_group_update_params, rgw_sync_shard_group_update_result>;

struct rgw_sync_shard_group_get_info_params {
  rgw_raw_obj key;
  std::optional<string> group_id;
};

struct rgw_sync_shard_group_get_info_result {
  std::vector<cls_rgw_sync_group_info> result;
};

using RGWSyncShardGroupGetInfoCR = RGWSimpleAsyncCR<rgw_sync_shard_group_get_info_params, rgw_sync_shard_group_get_info_result>;

struct rgw_sync_shard_group_list_params {
  rgw_raw_obj key;
  std::string group_id;
  std::optional<uint64_t> marker;
  uint32_t max_entries;
};

struct rgw_sync_shard_group_list_result {
  std::vector<std::pair<uint64_t, bool> > result;
  bool more{false};
};

using RGWSyncShardGroupListCR = RGWSimpleAsyncCR<rgw_sync_shard_group_list_params, rgw_sync_shard_group_list_result>;

struct rgw_sync_shard_group_purge_params {
  rgw_raw_obj key;
  std::string group_id;
};

using RGWSyncShardGroupPurgeCR = RGWSimpleWriteOnlyAsyncCR<rgw_sync_shard_group_purge_params>;

