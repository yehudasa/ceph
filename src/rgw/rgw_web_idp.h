#ifndef CEPH_RGW_WEB_IDP_H
#define CEPH_RGW_WEB_IDP_H

namespace rgw {
namespace web_idp {

//WebToken contains some claims from the decoded token which are of interest to us.
struct WebTokenClaims {
  //Subject of the token
  std::string sub;
  //Intended audience for this token
  std::string aud;
  //Issuer of this token
  std::string iss;
  //Human-readable id for the resource owner
  string user_name;
};

}; /* namespace web_idp */
}; /* namespace rgw */

#endif /* CEPH_RGW_WEB_IDP_H */
