// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/shard_services.h"

#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/os/cyan_store.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/os/cyan_store.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGQuery.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

ShardServices::ShardServices(
  ceph::net::Messenger &cluster_msgr,
  ceph::net::Messenger &public_msgr,
  ceph::mon::Client &monc,
  ceph::mgr::Client &mgrc,
  ceph::os::FuturizedStore &store)
    : cluster_msgr(cluster_msgr),
      public_msgr(public_msgr),
      monc(monc),
      mgrc(mgrc),
      store(store)
{
  perf = build_osd_logger(&cct);
  cct.get_perfcounters_collection()->add(perf);

  recoverystate_perf = build_recoverystate_perf(&cct);
  cct.get_perfcounters_collection()->add(recoverystate_perf);
}

seastar::future<> ShardServices::send_to_osd(
  int peer, Ref<Message> m, epoch_t from_epoch) {
  if (osdmap->is_down(peer) || osdmap->get_info(peer).up_from > from_epoch) {
    return seastar::now();
  } else {
    return cluster_msgr.connect(osdmap->get_cluster_addrs(peer).front(),
      CEPH_ENTITY_TYPE_OSD)
      .then([m, this] (auto xconn) {
	      return (*xconn)->send(m);
	    });
  }
}

seastar::future<> ShardServices::dispatch_context_transaction(
  ceph::os::CollectionRef col, PeeringCtx &ctx) {
  auto ret = store.do_transaction(
    col,
    std::move(ctx.transaction));
  ctx.reset_transaction();
  return ret;
}

seastar::future<> ShardServices::dispatch_context_messages(
  BufferedRecoveryMessages &&ctx)
{
  auto ret = seastar::when_all_succeed(
    seastar::parallel_for_each(std::move(ctx.notify_list),
      [this](auto& osd_notifies) {
	auto& [peer, notifies] = osd_notifies;
	auto m = make_message<MOSDPGNotify>(osdmap->get_epoch(),
					    std::move(notifies));
	logger().debug("dispatch_context_messages sending notify to {}", peer);
	return send_to_osd(peer, m, osdmap->get_epoch());
      }),
    seastar::parallel_for_each(std::move(ctx.query_map),
      [this](auto& osd_queries) {
	auto& [peer, queries] = osd_queries;
	auto m = make_message<MOSDPGQuery>(osdmap->get_epoch(),
					   std::move(queries));
	logger().debug("dispatch_context_messages sending query to {}", peer);
	return send_to_osd(peer, m, osdmap->get_epoch());
      }),
    seastar::parallel_for_each(std::move(ctx.info_map),
      [this](auto& osd_infos) {
	auto& [peer, infos] = osd_infos;
	auto m = make_message<MOSDPGInfo>(osdmap->get_epoch(),
					  std::move(infos));
	logger().debug("dispatch_context_messages sending info to {}", peer);
	return send_to_osd(peer, m, osdmap->get_epoch());
      }));
  ctx.notify_list.clear();
  ctx.query_map.clear();
  ctx.info_map.clear();
  return ret;
}

seastar::future<> ShardServices::dispatch_context(
  ceph::os::CollectionRef col,
  PeeringCtx &&ctx)
{
  ceph_assert(col || ctx.transaction.empty());
  return seastar::when_all_succeed(
    dispatch_context_messages(BufferedRecoveryMessages(ctx)),
    col ? dispatch_context_transaction(col, ctx) : seastar::now());
}

void ShardServices::queue_want_pg_temp(pg_t pgid,
				    const vector<int>& want,
				    bool forced)
{
  auto p = pg_temp_pending.find(pgid);
  if (p == pg_temp_pending.end() ||
      p->second.acting != want ||
      forced) {
    pg_temp_wanted[pgid] = {want, forced};
  }
}

void ShardServices::remove_want_pg_temp(pg_t pgid)
{
  pg_temp_wanted.erase(pgid);
  pg_temp_pending.erase(pgid);
}

void ShardServices::_sent_pg_temp()
{
#ifdef HAVE_STDLIB_MAP_SPLICING
  pg_temp_pending.merge(pg_temp_wanted);
#else
  pg_temp_pending.insert(make_move_iterator(begin(pg_temp_wanted)),
			 make_move_iterator(end(pg_temp_wanted)));
#endif
  pg_temp_wanted.clear();
}

void ShardServices::requeue_pg_temp()
{
  unsigned old_wanted = pg_temp_wanted.size();
  unsigned old_pending = pg_temp_pending.size();
  _sent_pg_temp();
  pg_temp_wanted.swap(pg_temp_pending);
  logger().debug(
    "{}: {} + {} -> {}",
    __func__ ,
    old_wanted,
    old_pending,
    pg_temp_wanted.size());
}

std::ostream& operator<<(
  std::ostream& out,
  const ShardServices::pg_temp_t& pg_temp)
{
  out << pg_temp.acting;
  if (pg_temp.forced) {
    out << " (forced)";
  }
  return out;
}

void ShardServices::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return;
  logger().debug("{}: {}", __func__, pg_temp_wanted);
  boost::intrusive_ptr<MOSDPGTemp> ms[2] = {nullptr, nullptr};
  for (auto& [pgid, pg_temp] : pg_temp_wanted) {
    auto m = ms[pg_temp.forced];
    if (!m) {
      m = make_message<MOSDPGTemp>(osdmap->get_epoch());
      m->forced = pg_temp.forced;
    }
    m->pg_temp.emplace(pgid, pg_temp.acting);
  }
  for (auto &m : ms) {
    if (m) {
      monc.send_message(m);
    }
  }
  _sent_pg_temp();
}

void ShardServices::update_map(cached_map_t new_osdmap)
{
  osdmap = std::move(new_osdmap);
}

ShardServices::cached_map_t &ShardServices::get_osdmap()
{
  return osdmap;
}

seastar::future<> ShardServices::send_pg_created(pg_t pgid)
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  pg_created.insert(pgid);
  return monc.send_message(make_message<MOSDPGCreated>(pgid));
}

seastar::future<> ShardServices::send_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  return seastar::parallel_for_each(pg_created,
    [this](auto &pgid) {
      return monc.send_message(make_message<MOSDPGCreated>(pgid));
    });
}

void ShardServices::prune_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  auto i = pg_created.begin();
  while (i != pg_created.end()) {
    auto p = o->get_pg_pool(i->pool());
    if (!p || !p->has_flag(pg_pool_t::FLAG_CREATING)) {
      logger().debug("{} pruning {}", __func__, *i);
      i = pg_created.erase(i);
    } else {
      logger().debug(" keeping {}", __func__, *i);
      ++i;
    }
  }
}

seastar::future<> ShardServices::osdmap_subscribe(version_t epoch, bool force_request)
{
  logger().info("{}({})", __func__, epoch);
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

};
