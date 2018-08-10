#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"


class CephContext;
class JSONFormattable;
class RGWServiceInstance;

using RGWServiceInstanceRef = std::shared_ptr<RGWServiceInstance>;

class RGWService
{
protected:
  CephContext *cct;
  std::string svc_type;

public:
  RGWService(CephContext *_cct, const std::string& _svc_type) : cct(_cct),
                                                           svc_type(_svc_type) {}
  virtual ~RGWService() = default;

  const std::string& type() {
    return svc_type;
  }
  virtual std::vector<std::string> deps() = 0;
  virtual int create_instance(JSONFormattable& conf, RGWServiceInstanceRef *instance) = 0;
};


using RGWServiceRef = std::shared_ptr<RGWService>;


class RGWServiceInstance
{
protected:
  CephContext *cct;
  string svc_type;
  string svc_instance;

  virtual int do_init(JSONFormattable& conf) = 0;
public:
  RGWServiceInstance(RGWService *svc, CephContext *_cct) : cct(_cct) {
    svc_type = svc->type();
  }

  virtual ~RGWServiceInstance() = default;

  int init(JSONFormattable& conf) {
    int r = do_init(conf);
    if (r < 0) {
      return r;
    }
    assert(!svc_instance.empty());
    return 0;
  }

  string get_id() {
    return svc_type + ":" + svc_instance;
  }
};

struct RGWServiceRegistry {
  static void init(CephContext *cct);
  static bool find(const string& name, RGWServiceRef *svc);
};

#endif
