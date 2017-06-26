// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>
#include <list>

#include "include/utime.h"
#include "include/buffer.h"
#include "msg/msg_types.h"

namespace ceph {
  class Formatter;
}

struct ServiceMap {
  struct Daemon {
    uint64_t gid = 0;
    entity_addr_t addr;
    epoch_t start_epoch = 0;   ///< epoch first registered
    utime_t start_stamp;       ///< timestamp daemon started/registered
    std::map<std::string,std::string> metadata;  ///< static metadata

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(std::list<Daemon*>& ls);
  };

  struct Service {
    map<std::string,Daemon> daemons;

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(std::list<Service*>& ls);
  };

  epoch_t epoch = 0;
  utime_t modified;
  map<std::string,Service> services;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ServiceMap*>& ls);

  Daemon* get_daemon(const std::string& service,
		     const std::string& daemon) {
    return &services[service].daemons[daemon];
  }

  bool rm_daemon(const std::string& service,
		 const std::string& daemon) {
    auto p = services.find(service);
    if (p == services.end()) {
      return false;
    }
    auto q = p->second.daemons.find(daemon);
    if (q == p->second.daemons.end()) {
      return false;
    }
    p->second.daemons.erase(q);
    if (p->second.daemons.empty()) {
      services.erase(p);
    }
    return true;
  }
};
WRITE_CLASS_ENCODER_FEATURES(ServiceMap)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Service)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Daemon)
