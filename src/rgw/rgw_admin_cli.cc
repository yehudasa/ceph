#include <map>

#include "global/global_init.h"
#include "global/global_context.h"

#include "common/ceph_argparse.h"

using namespace std;

static int usage(int r)
{
  cerr << "Usage: ..." << std::endl;
  return r;
}

class ParamsEnv {
  enum Type {
    UNKNOWN = -1,
    STR = 0,
    INT = 1,
    BOOL = 2,
  };

  struct Param {
    Type t;
    string p1;
    string p2;
  };

  std::map<std::string, Param> params;

public:
  bool declare_param(const string& name,
                     Type t,
                     string p1,
                     std::optional<string> p2) {
    auto iter = params.find(name);
    if (iter != params.end()) {
      if (iter->second.t != t ||
          iter->second.p1 != p1 ||
          iter->second.p2 != p2) {
        return false;
      }
    } else {
      params[name] = { t, p1, p2.value_or(string()) };
    }

    return true;
  }

  bool declare_param_str(const string& name,
                         string p1,
                         std::optional<string> p2 = std::nullopt) {
    return declare_param(name, Type::STR, p1, p2);
  }

  bool declare_param_int(const string& name,
                         string p1,
                         std::optional<string> p2 = std::nullopt) {
    return declare_param(name, Type::INT, p1, p2);
  }

  bool declare_param_bool(const string& name,
                         string p1,
                         std::optional<string> p2 = std::nullopt) {
    return declare_param(name, Type::BOOL, p1, p2);
  }

  std::optional<Param> find_param(const string& name) const {
    auto iter = params.find(name);
    if (iter == params.end()) {
      return std::nullopt;
    }
    return iter->second;
  }
};

class CLIEnv {
};

class RGWAdminModule_Period
{
  std::optional<std::string> period_id;

public:

  bool declare_params(ParamsEnv& params_env) {
    return params_env.declare_param_str("period_id",
                                        "--period-id");
  }

  struct OpDelete {

    OpDelete(CLIEnv& env) {
      // env.required_param("period_id", &period_id);
    }
  };

  int op_delete();
  int op_get();
  int op_get_current();
  int op_list();
  int op_update();
  int op_pull();
};

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, (const char **)argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    exit(usage(0));
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  // for region -> zonegroup conversion (must happen before common_init_finish())
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);
}
