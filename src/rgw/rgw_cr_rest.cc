#include "rgw_cr_rest.h"

#include "rgw_coroutine.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWCRHTTPGetDataCB : public RGWGetDataCB {
  Mutex lock;
  RGWCoroutinesEnv *env;
  RGWCoroutine *cr;
  bufferlist data;
public:
  RGWCRHTTPGetDataCB(RGWCoroutinesEnv *_env, RGWCoroutine *_cr) : lock("RGWCRHTTPGetDataCB"), env(_env), cr(_cr) {}

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

#if 0
    env->manager->set_sleeping(cr, false); /* wake up! */
#endif
    env->manager->io_complete(cr);
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


RGWStreamReadHTTPResourceCRF::~RGWStreamReadHTTPResourceCRF()
{
  delete in_cb;
}

int RGWStreamReadHTTPResourceCRF::init()
{
  in_cb = new RGWCRHTTPGetDataCB(env, caller);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStreamReadHTTPResourceCRF::read(bufferlist *out, uint64_t max_size)
{
  reenter(&read_state) {
    while (!req->is_done()) {
      if (!in_cb->has_data()) {
#if 0
        yield caller->set_sleeping(true);
#endif
        yield caller->io_block();
      }
      in_cb->claim_data(out, max_size);
    }
  }
  return 0;
}

TestCR::TestCR(CephContext *_cct, RGWHTTPManager *_mgr, RGWHTTPStreamRWRequest *_req) : RGWCoroutine(_cct), cct(_cct), http_manager(_mgr),
                                                                                        req(_req) {}
TestCR::~TestCR() {
  delete crf;
}

int TestCR::operate() {
  reenter(this) {
    crf = new RGWStreamReadHTTPResourceCRF(cct, get_env(), this, http_manager, req);

    {
      int ret = crf->init();
      if (ret < 0) {
        return set_cr_error(ret);
      }
    }

    do {

      bl.clear();

      yield {
        ret = crf->read(&bl, 4 * 1024 * 1024);
        if (ret < 0)  {
          return set_cr_error(ret);
        }
      }

      if (retcode < 0) {
        dout(0) << __FILE__ << ":" << __LINE__ << " retcode=" << retcode << dendl;
        return set_cr_error(ret);
      }

      dout(0) << "read " << bl.length() << " bytes" << dendl;
    } while (bl.length() > 0);

    return set_cr_done();
  }
  return 0;
}
