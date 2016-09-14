#include "rgw_global.h"

#include "rgw_tools.h"
#include "rgw_resolve.h"

int rgw_init_global_info(CephContext *cct, string *error)
{
  cct->priv = (void *)new rgw_global_info;

  int r = rgw_tools_init(cct);
  if (r < 0) {
    *error = "unable to initialize rgw tools";
    return -r;
  }

  rgw_init_resolver(cct);

  return 0;

}

void rgw_destroy_global_info(CephContext *cct)
{
  rgw_tools_cleanup(cct);
  rgw_shutdown_resolver(cct);
  delete (rgw_global_info *)cct->priv;
}

