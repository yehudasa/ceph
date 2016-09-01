// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ASYNC_COMPLETION_H
#define CEPH_RGW_ASYNC_COMPLETION_H

#include "common/RefCountedObj.h"

#include "rgw_common.h"
#include "common/Timer.h"


class RGWCompletionManager;
class RGWAioCompletionNotifier;
class RGWCoroutinesStack;

struct rgw_async_completion {
  RGWCompletionManager *manager{nullptr};
  void *handle{nullptr};

  rgw_async_completion() {}
  rgw_async_completion(RGWCompletionManager *_manager, void *_handle) : manager(_manager), handle(_handle) {}
};


class RGWCompletionManager : public RefCountedObject {
  CephContext *cct;
  list<void *> complete_reqs;
  set<RGWAioCompletionNotifier *> cns;

  Mutex lock;
  Cond cond;

  SafeTimer timer;

  atomic_t going_down;

  map<void *, void *> waiters;

  class WaitContext : public Context {
    RGWCompletionManager *manager;
    void *opaque;
  public:
    WaitContext(RGWCompletionManager *_cm, void *_opaque) : manager(_cm), opaque(_opaque) {}
    void finish(int r) {
      manager->_wakeup(opaque);
    }
  };

  friend class WaitContext;

protected:
  void _wakeup(void *opaque);
  void _complete(RGWAioCompletionNotifier *cn, void *completion_handle);
public:
  RGWCompletionManager(CephContext *_cct);
  ~RGWCompletionManager();

  void complete(RGWAioCompletionNotifier *cn, void *completion_handle);
  int get_next(void **completion_handle);
  bool try_get_next(void **completion_handle);

  void go_down();

  /*
   * wait for interval length to complete completion_handle
   */
  void wait_interval(void *opaque, const utime_t& interval, void *completion_handle);
  void wakeup(void *opaque);

  void register_completion_notifier(RGWAioCompletionNotifier *cn);
  void unregister_completion_notifier(RGWAioCompletionNotifier *cn);
};

/* a single use librados aio completion notifier that hooks into the RGWCompletionManager */
class RGWAioCompletionNotifier : public RefCountedObject {
  librados::AioCompletion *c;
  RGWCompletionManager *completion_mgr;
  void *user_data;
  Mutex lock;
  bool registered;

public:
  RGWAioCompletionNotifier(RGWCompletionManager *_mgr, void *_user_data);
  ~RGWAioCompletionNotifier() {
    c->release();
    lock.Lock();
    bool need_unregister = registered;
    if (registered) {
      completion_mgr->get();
    }
    registered = false;
    lock.Unlock();
    if (need_unregister) {
      completion_mgr->unregister_completion_notifier(this);
      completion_mgr->put();
    }
  }

  librados::AioCompletion *completion() {
    return c;
  }

  void unregister() {
    Mutex::Locker l(lock);
    if (!registered) {
      return;
    }
    registered = false;
  }

  void cb() {
    lock.Lock();
    if (!registered) {
      lock.Unlock();
      put();
      return;
    }
    completion_mgr->get();
    registered = false;
    lock.Unlock();
    completion_mgr->complete(this, user_data);
    completion_mgr->put();
    put();
  }
};

#endif

