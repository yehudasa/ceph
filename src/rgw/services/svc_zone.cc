#include "svc_zone.h"

#include "rgw/rgw_zone.h"

std::map<string, RGWServiceInstance::dependency> RGWSI_Zone::get_deps()
{
  RGWServiceInstance::dependency dep = { .name = "rados",
                                         .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["rados_dep"] = dep;
  return deps;
}

int RGWSI_Zone::init(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  svc_rados = dep_refs["rados_dep"];
  assert(svc_rados);
  return 0;
}

RGWZoneParams& RGWSI_Zone::get_zone_params()
{
  return *zone_params;
}

RGWZone& RGWSI_Zone::get_zone()
{
  return *zone_public_config;
}

RGWZoneGroup& RGWSI_Zone::get_zonegroup()
{
  return *zonegroup;
}

int RGWSI_Zone::get_zonegroup(const string& id, RGWZoneGroup& zonegroup)
{
  int ret = 0;
  if (id == get_zonegroup().get_id()) {
    zonegroup = get_zonegroup();
  } else if (!current_period->get_id().empty()) {
    ret = current_period->get_zonegroup(zonegroup, id);
  }
  return ret;
}

RGWRealm& RGWSI_Zone::get_realm()
{
  return *realm;
}

bool RGWSI_Zone::zone_is_writeable()
{
  return writeable_zone && !get_zone().is_read_only();
}

uint32_t RGWSI_Zone::get_zone_short_id() const
{
  return zone_short_id;
}

const string& RGWSI_Zone::zone_name()
{
  return get_zone_params().get_name();
}
const string& RGWSI_Zone::zone_id()
{
  return get_zone_params().get_id();
}
