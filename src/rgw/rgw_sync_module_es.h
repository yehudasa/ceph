#ifndef CEPH_RGW_SYNC_MODULE_ES_H
#define CEPH_RGW_SYNC_MODULE_ES_H

#include "rgw_sync_module.h"

class RGWRealm;

class RGWElasticSyncModule : public RGWSyncModule {
public:
  RGWElasticSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) override;
};

class RGWElasticDataSyncModule;
class RGWRESTConn;

class RGWElasticSyncModuleInstance : public RGWSyncModuleInstance {
  std::unique_ptr<RGWElasticDataSyncModule> data_handler;
public:
  RGWElasticSyncModuleInstance(CephContext *cct, const string& endpoint);
  RGWDataSyncModule *get_data_handler() override;
  RGWRESTMgr *get_rest_filter(int dialect, RGWRESTMgr *orig) override;
  RGWRESTConn *get_rest_conn();
  std::string get_index_path(const RGWRealm& realm);
};

#endif
