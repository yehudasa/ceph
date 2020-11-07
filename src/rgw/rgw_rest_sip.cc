// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_sip.h"
#include "rgw_rest_conn.h"
#include "rgw_sync_info.h"

#include "common/errno.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


void RGWOp_SIP_GetInfo::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");

  sip = store->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    http_ret = -ENOENT;
    return;
  }
}

void RGWOp_SIP_GetInfo::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("info", sip->get_info(), s->formatter);
  flusher.flush();
}

void RGWOp_SIP_List::execute() {
  providers = store->ctl()->si.mgr->list_sip();
}

void RGWOp_SIP_List::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("providers", providers, s->formatter);
  flusher.flush();
}

void RGWOp_SIP_Fetch::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");
  auto opt_stage_id = s->info.args.get_std_optional("stage-id");
  auto marker = s->info.args.get("marker");

  int max_entries;
  http_ret = s->info.args.get_int("max", &max_entries, default_max);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'max' param: " << http_ret << dendl;
    return;
  }

  int shard_id;
  http_ret = s->info.args.get_int("shard-id", &shard_id, 0);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'shard-id' param: " << http_ret << dendl;
    return;
  }

  sip = store->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    return;
  }

  stage_id = opt_stage_id.value_or(sip->get_first_stage());

  http_ret = sip->fetch(stage_id, shard_id, marker, max_entries, &result);
  if (http_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch entries: " << http_ret << dendl;
    return;
  }
}

void RGWOp_SIP_Fetch::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  auto formatter = s->formatter;

  {
    Formatter::ObjectSection top_section(*formatter, "result");
    encode_json("more", result.more, formatter);
    encode_json("done", result.done, formatter);

    Formatter::ArraySection as(*formatter, "entries");

    for (auto& e : result.entries) {
      Formatter::ObjectSection hs(*formatter, "handler");
      encode_json("key", e.key, formatter);
      int r = sip->handle_entry(stage_id, e, [&](SIProvider::EntryInfoBase& info) {
                                encode_json("info", info, formatter);
                                return 0;
                                });
      if (r < 0) {
        ldout(s->cct, 0) << "ERROR: provider->handle_entry() failed: r=" << r << dendl;
        break;
      }
    }
  }
  flusher.flush();
}

void RGWOp_SIP_Trim::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");
  auto opt_stage_id = s->info.args.get_std_optional("stage-id");
  auto marker = s->info.args.get("marker");

  int shard_id;
  http_ret = s->info.args.get_int("shard-id", &shard_id, 0);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'shard-id' param: " << http_ret << dendl;
    return;
  }

  auto sip = store->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    return;
  }

  auto stage_id = opt_stage_id.value_or(sip->get_first_stage());

  http_ret = sip->trim(stage_id, shard_id, marker);
  if (http_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch entries: " << http_ret << dendl;
    return;
  }
}

RGWOp *RGWHandler_SIP::op_get() {
  auto provider = s->info.args.get_std_optional("provider");
  if (!provider) {
    return new RGWOp_SIP_List;
  }

  if (s->info.args.exists("info")) {
    return new RGWOp_SIP_GetInfo(std::move(*provider));
  }

  return new RGWOp_SIP_Fetch(std::move(*provider));
}

RGWOp *RGWHandler_SIP::op_delete() {
  auto provider = s->info.args.get_std_optional("provider");
  if (!provider) {
    return nullptr;
  }

  return new RGWOp_SIP_Trim(std::move(*provider));
}

