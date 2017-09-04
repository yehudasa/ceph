#include "rgw_cr_rest.h"

#include "rgw_coroutine.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#include <boost/asio/yield.hpp>

class RGWCRRESTGetDataCB : public RGWGetDataCB {
  Mutex lock;
  RGWCoroutinesEnv *env;
  RGWCoroutine *cr;
  bufferlist data;
public:
  RGWCRRESTGetDataCB(RGWCoroutinesEnv *_env, RGWCoroutine *_cr) : lock("RGWCRRESTGetDataCB"), env(_env), cr(_cr) {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    {
      Mutex::Locker l(lock);
      if (!bl_ofs && bl_len == bl.length()) {
        data.claim_append(bl);
      } else {
        bufferptr bp(bl.c_str() + bl_ofs, bl_len);
        data.push_back(bp);
      }
    }

    env->manager->set_sleeping(cr, false); /* wake up! */
    return 0;
  }

  void claim_data(bufferlist *dest, uint64_t max) {
    Mutex::Locker l(lock);

    if (data.length() == 0) {
      return;
    }

    if (data.length() < max) {
      max = data.length();
    }

    data.splice(0, max, dest);
  }

  bool has_data() {
    return (data.length() > 0);
  }
};


RGWStreamReadRESTResourceCRF::~RGWStreamReadRESTResourceCRF()
{
  delete in_cb;
}

int RGWStreamReadRESTResourceCRF::init()
{
  in_cb = new RGWCRRESTGetDataCB(env, caller);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStreamReadRESTResourceCRF::read(bufferlist *out, uint64_t max_size)
{
  reenter(&read_state) {
    while (!req->is_done()) {
      if (!in_cb->has_data()) {
        yield caller->set_sleeping(true);
      }
      in_cb->claim_data(out, max_size);
    }
  }
  return 0;
}

