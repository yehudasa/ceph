#ifndef CEPH_RGW_SYNC_MODULE_ARCHIVE_H
#define CEPH_RGW_SYNC_MODULE_ARCHIVE_H

#include "rgw_sync_module.h"
#include "rgw_data_sync.h"


class RGWArchiveSyncModule : public RGWDefaultSyncModule {
public:
  RGWArchiveSyncModule() {}
  bool supports_data_export() override { return false; }
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif
