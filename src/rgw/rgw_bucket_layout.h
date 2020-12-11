// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <optional>
#include <string>
#include "include/encoding.h"

namespace ceph {
  class Formatter;
} // namespace ceph

namespace rgw {

enum class BucketIndexType : uint8_t {
  Normal, // normal hash-based sharded index layout
  Indexless, // no bucket index, so listing is unsupported
};

enum class BucketHashType : uint8_t {
  Mod, // rjenkins hash of object name, modulo num_shards
};

inline std::string bucket_index_type_to_str(const BucketIndexType& index_type) {
  switch (index_type) {
    case BucketIndexType::Normal:
      return "Normal";
    case BucketIndexType::Indexless:
      return "Indexless";
    default:
      return "Unknown";
  }
}

inline std::ostream& operator<<(std::ostream& out, const BucketIndexType &index_type)
{
  return out << bucket_index_type_to_str(index_type);
}

inline std::string bucket_hash_type_to_str(const BucketHashType& hash_type) {
  switch (hash_type) {
    case BucketHashType::Mod:
      return "Mod";
    default:
      return "Unknown";
  }
}

struct bucket_index_normal_layout {
  uint32_t num_shards = 1;

  BucketHashType hash_type = BucketHashType::Mod;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl);


struct bucket_index_layout {
  BucketIndexType type = BucketIndexType::Normal;

  // TODO: variant of layout types?
  bucket_index_normal_layout normal;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl);


struct bucket_index_layout_generation {
  uint64_t gen = 0;
  bucket_index_layout layout;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_index_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout_generation& l, bufferlist::const_iterator& bl);


enum class BucketLogType : uint8_t {
  // colocated with bucket index, so the log layout matches the index layout
  InIndex,
};

inline std::string bucket_log_type_to_str(const BucketLogType& log_type) {
  switch (log_type) {
    case BucketLogType::InIndex:
      return "InIndex";
    default:
      return "Unknown";
  }
}

inline std::ostream& operator<<(std::ostream& out, const BucketLogType &log_type)
{
  return out << bucket_log_type_to_str(log_type);
}

struct bucket_index_log_layout {
  uint64_t gen = 0;
  bucket_index_normal_layout layout;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_index_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_log_layout& l, bufferlist::const_iterator& bl);

struct bucket_log_layout {
  BucketLogType type = BucketLogType::InIndex;

  bucket_index_log_layout in_index;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout& l, bufferlist::const_iterator& bl);


struct bucket_log_layout_generation {
  uint64_t gen = 0;
  bucket_log_layout layout;

  void dump(ceph::Formatter *f) const;
};

void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl);

// return a log layout that shares its layout with the index
inline bucket_log_layout_generation log_layout_from_index(
    uint64_t gen, const bucket_index_normal_layout& index)
{
  return {gen, {BucketLogType::InIndex, {gen, index}}};
}

enum class BucketReshardState : uint8_t {
  NONE,
  IN_PROGRESS,
};

inline std::string bucket_reshard_state_to_str(const BucketReshardState& reshard_state) {
  switch (reshard_state) {
    case BucketReshardState::NONE:
      return "none";
    case BucketReshardState::IN_PROGRESS:
      return "in-progress";
    default:
      return "unknown";
  }
}

// describes the layout of bucket index objects
struct BucketLayout {
  BucketReshardState resharding = BucketReshardState::NONE;

  // current bucket index layout
  bucket_index_layout_generation current_index;

  // target index layout of a resharding operation
  std::optional<bucket_index_layout_generation> target_index;

  // history of untrimmed bucket log layout generations, with the current
  // generation at the back()
  std::map<uint64_t, bucket_log_layout_generation> logs;

  void dump(ceph::Formatter *f) const;
};

void encode(const BucketLayout& l, bufferlist& bl, uint64_t f=0);
void decode(BucketLayout& l, bufferlist::const_iterator& bl);

inline uint32_t current_num_shards(const BucketLayout& layout) {
  return std::max(layout.current_index.layout.normal.num_shards, 1u);
  }

} // namespace rgw
