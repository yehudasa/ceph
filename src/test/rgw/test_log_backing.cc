// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_log_backing.h"

#include <cerrno>
#include <iostream>
#include <string_view>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "cls/log/cls_log_client.h"

#include "rgw/rgw_tools.h"
#include "rgw/cls_fifo_legacy.h"

#include "gtest/gtest.h"

namespace lr = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;

class LogBacking : public testing::Test {
protected:
  static constexpr int SHARDS = 3;
  const std::string pool_name = get_temp_pool_name();
  lr::Rados rados;
  lr::IoCtx ioctx;

  void SetUp() override {
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() override {
    destroy_one_pool_pp(pool_name, rados);
  }

  static std::string get_oid(int i) {
    return fmt::format("shard.{}", i);
  }

  void make_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      using ceph::encode;
      lr::ObjectWriteOperation op;
      cb::list bl;
      encode(i, bl);
      cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
      auto r = rgw_rados_operate(ioctx, get_oid(i), &op, null_yield);
      ASSERT_GE(r, 0);
    }
  }

  void add_omap(int i) {
    using ceph::encode;
    lr::ObjectWriteOperation op;
    cb::list bl;
    encode(i, bl);
    cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
    auto r = rgw_rados_operate(ioctx, get_oid(i), &op, null_yield);
    ASSERT_GE(r, 0);
  }

  void empty_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      auto oid = get_oid(i);
      std::string to_marker;
      {
	lr::ObjectReadOperation op;
	std::list<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_FALSE(entries.empty());
      }
      {
	lr::ObjectWriteOperation op;
	cls_log_trim(op, {}, {}, {}, to_marker);
	auto r = rgw_rados_operate(ioctx, oid, &op, null_yield);
	ASSERT_GE(r, 0);
      }
      {
	lr::ObjectReadOperation op;
	std::list<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_TRUE(entries.empty());
      }
    }
  }

  void make_fifo()
    {
      for (int i = 0; i < SHARDS; ++i) {
	std::unique_ptr<RCf::FIFO> fifo;
	auto r = RCf::FIFO::create(ioctx, get_oid(i), &fifo, null_yield);
	ASSERT_EQ(0, r);
	ASSERT_TRUE(fifo);
      }
    }

  void add_fifo(int i)
    {
      using ceph::encode;
      std::unique_ptr<RCf::FIFO> fifo;
      auto r = RCf::FIFO::open(ioctx, get_oid(i), &fifo, null_yield);
      ASSERT_GE(0, r);
      ASSERT_TRUE(fifo);
      cb::list bl;
      encode(i, bl);
      r = fifo->push(bl, null_yield);
      ASSERT_GE(0, r);
    }

  void assert_empty() {
    std::vector<lr::ObjectItem> result;
    lr::ObjectCursor next;
    auto r = ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(),
			       100, {}, &result, &next);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(result.empty());
  }
};

TEST_F(LogBacking, TestOmap)
{
  make_omap();
  // No mark, so all three should be false
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::omap,
						      log_type::fifo,
						      SHARDS,
						      get_oid,
						      null_yield);
  ASSERT_EQ(log_check::concord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::discord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  // Mark will have been deleted
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::neither,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));


  // Empty out omap.

  empty_omap();
  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  // Check that we still show there are entries when all but the
  // middle are empty.
  add_omap(1);
  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
						  null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::discord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestFifo)
{
  make_fifo();
  // No mark, so all three should be false
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::fifo,
						      log_type::fifo,
						      SHARDS,
						      get_oid,
						      null_yield);
  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::discord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  // Mark will have been deleted
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::neither,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  // Add an entry

  add_fifo(1);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::discord, stat);
  ASSERT_TRUE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestBiasOmap)
{
  // Nothing exists
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::neither,
						      log_type::omap,
						      SHARDS,
						      get_oid,
						      null_yield);
  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));


  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::omap,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::omap, found);

  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::omap,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestBiasFifo)
{
  // Nothing exists
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx,log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::neither,
						      log_type::fifo,
						      SHARDS,
						      get_oid,
						      null_yield);
  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);

  ASSERT_EQ(log_check::concord, stat);
  ASSERT_FALSE(has_entries);
  ASSERT_EQ(log_type::fifo, found);

  ASSERT_EQ(log_type::neither,
	    log_quick_check(ioctx, log_type::omap, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::fifo, get_oid, null_yield));
  ASSERT_EQ(log_type::fifo,
	    log_quick_check(ioctx, log_type::neither, get_oid, null_yield));


  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestMixed) {
  {
    std::unique_ptr<RCf::FIFO> fifo;
    auto r = RCf::FIFO::create(ioctx, get_oid(0), &fifo, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(fifo);
  }
  {
    std::unique_ptr<RCf::FIFO> fifo;
    auto r = RCf::FIFO::create(ioctx, get_oid(2), &fifo, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(fifo);
  }
  add_omap(1);

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::neither,
						      log_type::fifo,
						      SHARDS,
						      get_oid,
						      null_yield);

  ASSERT_EQ(log_check::corruption, stat);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::corruption, stat);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 SHARDS,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::corruption, stat);

  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestCorruptShard) {
  {
    std::unique_ptr<RCf::FIFO> fifo;
    auto r = RCf::FIFO::create(ioctx, get_oid(0), &fifo, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(fifo);
  }
  add_omap(0);

  auto [stat, has_entries, found] = log_setup_backing(ioctx,
						      log_type::neither,
						      log_type::fifo,
						      1,
						      get_oid,
						      null_yield);

  ASSERT_EQ(log_check::corruption, stat);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::omap,
							 log_type::fifo,
							 1,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::corruption, stat);

  std::tie(stat, has_entries, found) = log_setup_backing(ioctx,
							 log_type::fifo,
							 log_type::fifo,
							 1,
							 get_oid,
							 null_yield);
  ASSERT_EQ(log_check::corruption, stat);

  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestFifoFromNeither) {
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::fifo,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestFifoFromFifo) {
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::fifo,
				     log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::fifo,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);
  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestOmapFromNeither) {
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::omap, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::omap,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestOmapFromOmap) {
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::omap,
				   log_type::omap, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  found = log_acquire_backing(ioctx, SHARDS, log_type::omap,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);
  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestEmptyFifoToOmap) {
  make_fifo();
  // Neither specified, should stay fifo.
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::omap, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);

  // Specified, should become omap.
  found = log_acquire_backing(ioctx, SHARDS, log_type::omap,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);


  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestEmptyOmapToFifo) {
  make_omap();
  empty_omap();
  // Neither specified, should stay omap.
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);

  // Specified, should become fifo.
  found = log_acquire_backing(ioctx, SHARDS, log_type::fifo,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);


  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}

TEST_F(LogBacking, TestNonEmptyFifoToOmap) {
  make_fifo();
  add_fifo(1);
  // Neither specified, should stay fifo.
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::omap, get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, found);

  // Omap Specified, should fail.
  found = log_acquire_backing(ioctx, SHARDS, log_type::omap,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::neither, found);
}

TEST_F(LogBacking, TestNonEmptyOmapToFifo) {
  make_omap();
  // Neither specified, should stay omap.
  auto found = log_acquire_backing(ioctx, SHARDS, log_type::neither,
				   log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::omap, found);

  // Fifo Specified, should fail.
  found = log_acquire_backing(ioctx, SHARDS, log_type::fifo,
			      log_type::fifo, get_oid, null_yield);
  ASSERT_EQ(log_type::neither, found);


  auto r = log_remove(ioctx, SHARDS, get_oid, null_yield);
  ASSERT_GE(r, 0);
  assert_empty();
}
