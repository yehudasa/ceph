#ifndef CEPH_RGW_SYNC_MODULE_S3EXPORT_H
#define CEPH_RGW_SYNC_MODULE_S3EXPORT_H

#include "rgw_sync_module.h"

class RGWS3ExportSyncModule : public RGWSyncModule {
public:
  RGWS3ExportSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif
