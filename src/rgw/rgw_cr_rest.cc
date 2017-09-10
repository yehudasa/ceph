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
dout(0) << __FILE__ << ":" << __LINE__ << ": bl.length()=" << bl.length() << " bl_ofs=" << bl_ofs << " bl_len=" << bl_len << " bl=" << string(bl.c_str(), bl.length()) << dendl; 
    {
      Mutex::Locker l(lock);
      if (bl_len == bl.length()) {
        data.claim_append(bl);
      } else {
        bl.splice(0, bl_len, &data);
      }
    }

    env->manager->io_complete(cr);
    return 0;
  }

  void claim_data(bufferlist *dest, uint64_t max) {
dout(0) << __FILE__ << ":" << __LINE__ << ": data.length()=" << data.length() << " data=" << string(data.c_str(), data.length()) << dendl;
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


RGWStreamRWHTTPResourceCRF::~RGWStreamRWHTTPResourceCRF()
{
  delete in_cb;
}

int RGWStreamRWHTTPResourceCRF::init()
{
  in_cb = new RGWCRHTTPGetDataCB(env, caller);

  req->set_user_info(env->stack);
  req->set_in_cb(in_cb);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStreamRWHTTPResourceCRF::read(bufferlist *out, uint64_t max_size, bool *need_retry)
{
    reenter(&read_state) {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
    while (!req->is_done()) {
      *need_retry = true;
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
      if (!in_cb->has_data()) {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
        yield caller->io_block();
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
      }
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
      *need_retry = false;
      in_cb->claim_data(out, max_size);
      if (!req->is_done()) {
        yield;
      }
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
    }
  }
  return 0;
}

int RGWStreamRWHTTPResourceCRF::write(bufferlist& data)
{
#warning write need to throttle and block
  reenter(&write_state) {
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
    while (!req->is_done()) {
dout(0) << __FILE__ << ":" << __LINE__ << " data.length()=" << data.length() <<  " data=" << string(data.c_str(), data.length()) << dendl;
      yield req->add_send_data(data);
dout(0) << __FILE__ << ":" << __LINE__ << dendl;
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
    crf = new RGWStreamRWHTTPResourceCRF(cct, get_env(), this, http_manager, req);

    {
      int ret = crf->init();
      if (ret < 0) {
        return set_cr_error(ret);
      }
    }

    do {

      bl.clear();

      do {
        yield {
          ret = crf->read(&bl, 4 * 1024 * 1024, &need_retry);
          if (ret < 0)  {
            return set_cr_error(ret);
          }
        }
      } while (need_retry);

      if (retcode < 0) {
        dout(0) << __FILE__ << ":" << __LINE__ << " retcode=" << retcode << dendl;
        return set_cr_error(ret);
      }

      dout(0) << "read " << bl.length() << " bytes" << dendl;

      if (bl.length() == 0) {
        break;
      }

      yield {
        ret = crf->write(bl);
        if (ret < 0)  {
          return set_cr_error(ret);
        }
      }

      if (retcode < 0) {
        dout(0) << __FILE__ << ":" << __LINE__ << " retcode=" << retcode << dendl;
        return set_cr_error(ret);
      }

      dout(0) << "wrote " << bl.length() << " bytes" << dendl;
    } while (true);

    return set_cr_done();
  }
  return 0;
}
