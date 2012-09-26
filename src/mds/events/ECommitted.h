// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MDS_ECOMMITTED_H
#define CEPH_MDS_ECOMMITTED_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ECommitted : public LogEvent {
public:
  metareqid_t reqid;

  ECommitted() : LogEvent(EVENT_COMMITTED) { }
  ECommitted(metareqid_t r) : 
    LogEvent(EVENT_COMMITTED), reqid(r) { }

  void print(ostream& out) {
    out << "ECommitted " << reqid;
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 3, bl);
    ::encode(struct_v, bl);
    ::encode(stamp, bl);
    ::encode(reqid, bl);
    ENCODE_FINISH(bl);
  } 
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    if (struct_v >= 2)
      ::decode(stamp, bl);
    ::decode(reqid, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_stream("stamp") << stamp;
    f->dump_stream("reqid") << reqid;
  }
  static void generate_test_instances(list<ECommitted*>& ls) {
    ls.push_back(new ECommitted);
    ls.push_back(new ECommitted);
    ls.back()->stamp = utime_t(1, 2);
    ls.back()->reqid = metareqid_t(entity_name_t::CLIENT(123), 456);
  }

  void update_segment() {}
  void replay(MDS *mds);
};

#endif
