// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_RATEMEASURE_H
#define CEPH_COMMON_RATEMEASURE_H

#include <deque>
#include "include/utime.h"
#include "common/Clock.h"
#include "common/Formatter.h"

namespace ceph {

class RateMeasure {
  struct TimeSample {
    time_t stamp;
    int64_t value;
    TimeSample(time_t s=0, int64_t v=0) : stamp(s), value(v) {}
  };
  std::deque<TimeSample> m_samples;
  unsigned m_max_samples, m_precision;
  time_t m_last_get_qt;
  int64_t m_last_get_value;

public:
  RateMeasure(unsigned m = 30, unsigned p = 5)
    : m_max_samples(m),
      m_precision(p),
      m_last_get_qt(0),
      m_last_get_value(0)
  { }

  void trim(utime_t stamp) {
    time_t max = stamp.sec() - m_precision * (m_max_samples + 1);
    while (!m_samples.empty() &&
	   m_samples.front().stamp < max) {
      m_samples.pop_front();
    }
  }

  void add(utime_t stamp, int64_t value) {
    time_t offset = stamp.sec() % m_precision;
    time_t qt = stamp.sec() - offset;
    if (!m_samples.empty() && m_samples.back().stamp == qt)
      m_samples.back().value += value;
    else
      m_samples.push_back(TimeSample(qt, value));
    trim(stamp);
  }

  int64_t get_rate(utime_t now, unsigned duration) {
    time_t offset = now.sec() % m_precision;
    time_t qt = now.sec() - offset;

    if (m_samples.size() < 2)
      return 0;

    if (qt == m_last_get_qt)
      return m_last_get_value;

    //std::cout << "qt " << qt << " offset " << offset << std::endl;
    trim(now);
    int64_t r = 0;
    int left = duration;
    time_t min_stamp = 0;
    std::deque<TimeSample>::const_reverse_iterator p = m_samples.rbegin();
    assert(p != m_samples.rend());
    ++p;
    for ( ; left > 0 && p != m_samples.rend(); left -= m_precision, ++p) {
      //std::cout << " t " << p->stamp << " v " << p->value << std::endl;
      r += p->value;
      min_stamp = p->stamp;
    }

    // scale up overall value if we have limited samples.
    if (qt - min_stamp < duration) {
      //std::cout << " scale total " << r << " by " << duration << " / " << (qt - min_stamp) << std::endl;
      r = r * duration / (qt - min_stamp);
    }

    // cache result for this time interval
    m_last_get_qt = qt;
    m_last_get_value = r;
    return r;
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("max_samples", m_max_samples);
    f->dump_unsigned("precision", m_precision);
    f->open_object_section("samples");
    for (std::deque<TimeSample>::const_iterator p = m_samples.begin();
	 p != m_samples.end();
	 ++p) {
      f->open_object_section("sample");
      f->dump_unsigned("stamp", p->stamp);
      f->dump_int("value", p->value);
      f->close_section();
    }
    f->close_section();
  }
};

}

#endif
