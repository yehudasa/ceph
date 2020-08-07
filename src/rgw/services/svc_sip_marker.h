// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class SIProvider;
using SIProviderRef = std::shared_ptr<SIProvider>;

namespace ceph {
  class Formatter;
}


class RGWSI_SIP_Marker : public RGWServiceInstance
{
public:

  using stage_id_t = string;

  struct client_marker_info {
    string pos;
    ceph::real_time mtime;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(pos, bl);
      encode(mtime, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(pos, bl);
      decode(mtime, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const;
  };

  struct stage_shard_info {
    map<string, client_marker_info> clients;
    std::string min_clients_pos;
    std::string low_pos;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(clients, bl);
      encode(min_clients_pos, bl);
      encode(low_pos, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(clients, bl);
      decode(min_clients_pos, bl);
      decode(low_pos, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const;
  };

  class Handler {
  public:
    virtual ~Handler() {}

    struct set_result {
      bool modified{false};
      std::optional<std::string> min_pos;
    };

    virtual int set_marker(const string& client_id,
                           const stage_id_t& sid,
                           int shard_id,
                           const std::string& marker,
                           const ceph::real_time& mtime,
                           bool init_client,
                           set_result *result) = 0;

    virtual int set_low_pos(const stage_id_t& sid,
                            int shard_id,
                            const std::string& pos) = 0;

    virtual int get_min_clients_pos(const stage_id_t& sid,
                                    int shard_id,
                                    std::optional<std::string> *pos) = 0;

    virtual int get_info(const stage_id_t& sid,
                         int shard_id,
                         stage_shard_info *info) = 0;
  };

  using HandlerRef = std::shared_ptr<Handler>;

  RGWSI_SIP_Marker(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_SIP_Marker() {}

  virtual HandlerRef get_handler(SIProviderRef& sip) = 0;
};
WRITE_CLASS_ENCODER(RGWSI_SIP_Marker::client_marker_info)
WRITE_CLASS_ENCODER(RGWSI_SIP_Marker::stage_shard_info)
