
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"

namespace ceph {
  class Formatter;
}

struct siprovider_meta_info : public SIProvider::EntryInfoBase {
  std::string section;
  std::string id;

  siprovider_meta_info() {}
  siprovider_meta_info(const string& _section, const string& _id) : section(_section),
                                                                    id(_id) {}

  string get_data_type() const override {
    return "meta";
  }

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(section, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
     DECODE_START(1, bl);
     decode(section, bl);
     decode(id, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const override;
  void decode_json(JSONObj *obj) override;
};
WRITE_CLASS_ENCODER(siprovider_meta_info)

class RGWMetadataManager;

class SIProvider_MetaFull : public SIProvider_SingleStage
{
  struct {
    RGWMetadataManager *mgr;
  } meta;

  std::list<std::string> sections;
  std::map<std::string, std::string> next_section_map;

  void append_section_from_set(std::set<std::string>& all_sections, const std::string& name);
  void rearrange_sections();
  int get_all_sections(const DoutPrefixProvider *dpp);

  int next_section(const DoutPrefixProvider *dpp,
                   const std::string& section, string *next);

protected:
  int do_fetch(const DoutPrefixProvider *dpp,
               int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(const DoutPrefixProvider *dpp,
                          int shard_id, std::string *marker, ceph::real_time *timestamp) const override {
    marker->clear();
    *timestamp = ceph::real_time();
    return 0;
  }

  int do_get_cur_state(const DoutPrefixProvider *dpp,
                       int shard_id, std::string *marker, ceph::real_time *timestamp, bool *disabled, optional_yield y) const {
    marker->clear(); /* full data, no current incremental state */
    *timestamp = ceph::real_time();
    *disabled = false;
    return 0;
  }


  int do_trim(const DoutPrefixProvider *dpp,
              int shard_id, const std::string& marker) override {
    return 0;
  }

public:
  SIProvider_MetaFull(CephContext *_cct,
                      RGWMetadataManager *meta_mgr) : SIProvider_SingleStage(_cct,
									     "meta.full",
                                                                             std::nullopt,
                                                                             std::make_shared<SITypeHandlerProvider_Default<siprovider_meta_info> >(),
                                                                             std::nullopt, /* stage id */
									     SIProvider::StageType::FULL,
									     1,
                                                                             false) {
    meta.mgr = meta_mgr;
  }

  int init(const DoutPrefixProvider *dpp);

  int next_meta_section(const std::string& cur_section, std::string *next) const;

  std::string to_marker(const std::string& section, const std::string& k) const;

  SIProvider::Entry create_entry(const std::string& section,
                                 const std::string& k,
                                 const std::string& m) const {
    siprovider_meta_info meta_info = { section, k };
    SIProvider::Entry e;
    e.key = to_marker(section, m);
    meta_info.encode(e.data);
    return e;
  }
};

class RGWSI_MDLog;
class RGWMetadataLog;

class SIProvider_MetaInc : public SIProvider_SingleStage
{
  RGWSI_MDLog *mdlog;
  string period_id;

  RGWMetadataLog *meta_log{nullptr};

protected:
  int do_fetch(const DoutPrefixProvider *dpp,
               int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(const DoutPrefixProvider *dpp,
                          int shard_id, std::string *marker, ceph::real_time *timestamp) const override;
  int do_get_cur_state(const DoutPrefixProvider *dpp,
                       int shard_id, std::string *marke, ceph::real_time *timestamp,
                       bool *disabled, optional_yield y) const;

  int do_trim(const DoutPrefixProvider *dpp,
              int shard_id, const std::string& marker) override;
public:
  SIProvider_MetaInc(CephContext *_cct,
                     RGWSI_MDLog *_mdlog,
                     const string& _period_id);

  int init(const DoutPrefixProvider *dpp);
};

