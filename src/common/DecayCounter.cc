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

#include "DecayCounter.h"

void DecayCounter::encode(bufferlist& bl) const
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(val, bl);
  ::encode(delta, bl);
  ::encode(vel, bl);
}

void DecayCounter::decode(const utime_t &t, bufferlist::iterator &p)
{
  __u8 struct_v;
  ::decode(struct_v, p);
  if (struct_v < 2) {
    double half_life;
    ::decode(half_life, p);
  }
  if (struct_v < 3) {
    double k;
    ::decode(k, p);
  }
  ::decode(val, p);
  ::decode(delta, p);
  ::decode(vel, p);
}

void DecayCounter::decay(utime_t now, const DecayRate &rate)
{
  utime_t el = now;
  el -= last_decay;

  if (el.sec() >= 1) {
    // calculate new value
    double newval = (val+delta) * exp((double)el * rate.k);
    if (newval < .01)
      newval = 0.0;

    // calculate velocity approx
    vel += (newval - val) * (double)el;
    vel *= exp((double)el * rate.k);

    val = newval;
    delta = 0;
    last_decay = now;
  }
}
