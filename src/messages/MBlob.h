// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef MBLOBH
#define MBLOBH

#include "msg/Message.h"
#include "include/utime.h"

class MBlob : public Message {
public:
  bufferlist bl;
  utime_t time;
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;
  MBlob()
    : Message(M_BLOB, HEAD_VERSION, COMPAT_VERSION) {}
  MBlob(bufferlist &bl, utime_t sent)
    : Message(M_BLOB, HEAD_VERSION, COMPAT_VERSION),
      bl(bl), time(sent) {}

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(bl, p);
    ::decode(time, p);
  }
  void encode_payload(uint64_t) {
    ::encode(bl, payload);
    ::encode(time, payload);
  }
  const char *get_type_name() const { return "MBlob"; }
  void print(ostream& out) const {
    out << "MBLob(len=" << bl.length() << ")";
  }
};


#endif
