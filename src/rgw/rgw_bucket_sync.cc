

#include "rgw_common.h"
#include "rgw_bucket_sync.h"



void RGWBucketSyncPolicy::post_init()
{
  source_zones.clear();
  for (auto& t : targets) {
    for (auto& r : t.second.rules) {
      source_zones.insert(r.source_zone_id);
    }
  }
}

vector<rgw_bucket_sync_pipe> rgw_bucket_sync_target_info::build_pipes(const rgw_bucket& source_bs)
{
  vector<rgw_bucket_sync_pipe> pipes;

  for (auto t : targets) {
    rgw_bucket_sync_pipe pipe;
    pipe.source_bs = source_bs;
    pipe.source_prefix = t.source_prefix;
    pipe.dest_prefix = t.dest_prefix;
    pipes.push_back(std::move(pipe));
  }
  return pipes;
}
