#include "rgw_service.h"

#include "services/svc_rados.h"


static map<string, RGWServiceRef> services;

void RGWServiceRegistry::init(CephContext *cct)
{
  services["rados"] = make_shared<RGWS_RADOS>(cct);
}

bool RGWServiceRegistry::find(const string& name, RGWServiceRef *svc)
{
  auto iter = services.find(name);
  if (iter == services.end()) {
    return false;
  }

  *svc = iter->second;
  return true;
}
