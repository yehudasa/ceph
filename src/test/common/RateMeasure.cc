#include "gtest/gtest.h"

#include "common/RateMeasure.h"

TEST(RateMeasure, empty) {
  utime_t now = ceph_clock_now(NULL);
  RateMeasure rm;
  ASSERT_EQ(0, rm.get_rate(now, 60));
}

TEST(RateMeasure, Constant) {
  RateMeasure rm(60, 5);
  for (int x = 1; x < 1000; x++)
    rm.add(utime_t(x,0), 10);
  ASSERT_EQ(600, rm.get_rate(utime_t(999, 0), 60));
}

TEST(RateMeasure, Incomplete) {
  for (int dur = 5; dur < 100; dur++) {
    RateMeasure rm(60, 5);
    for (int t = 0; t <= dur; t++)
      rm.add(utime_t(t, 0), 10);
    //std::cout << " duration " << dur << std::endl;
    ASSERT_EQ(600, rm.get_rate(utime_t(dur, 0), 60));
  }
}

TEST(RateMeasure, IncompleteExtrapolate) {
  for (int dur = 5; dur < 100; dur++) {
    RateMeasure rm(30, 5);
    for (int t = 0; t <= dur; t++)
      rm.add(utime_t(t, 0), 10);
    //std::cout << " duration " << dur << std::endl;
    ASSERT_EQ(600, rm.get_rate(utime_t(dur, 0), 60));
  }
}

