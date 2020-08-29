/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "proto_rndv.h"

#include <ucp/core/ucp_request.inl>
#include <ucp/proto/proto_multi.inl>


void ucp_rndv_req_send_ats(ucp_request_t *rndv_req, ucp_request_t *rreq,
                           ucs_ptr_map_key_t remote_req_id, ucs_status_t status);

static void
ucp_proto_rndv_get_zcopy_completion(uct_completion_t *self, ucs_status_t status)
{
    ucp_request_t *req      = ucs_container_of(self, ucp_request_t,
                                               send.state.uct_comp);
    ucp_request_t *recv_req = req->send.remote_op.rndv_recv.recv_req;

    ucp_rkey_destroy(req->send.remote_op.rkey);
    ucp_proto_request_zcopy_cleanup(req);
    ucp_rndv_req_send_ats(req, recv_req, req->send.remote_op.remote_request,
                          status);
    ucp_request_complete_tag_recv(recv_req, status);
    /* req will be released by ATS protocol
     * TODO move ATS sending to this file
     */
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_proto_rndv_get_zcopy_send_func(ucp_request_t *req,
                                   const ucp_proto_multi_lane_priv_t *lpriv,
                                   ucp_datatype_iter_t *next_iter)
{
    ucp_rkey_h rkey    = req->send.remote_op.rkey;
    uct_rkey_t tl_rkey = rkey->tl_rkey[lpriv->super.rkey_index].rkey.rkey;
    uct_iov_t iov;

    ucp_datatype_iter_next_iov(&req->send.dt_iter, lpriv->super.memh_index,
                               ucp_proto_multi_max_payload(req, lpriv, 0),
                               next_iter, &iov);
    return uct_ep_get_zcopy(req->send.ep->uct_eps[lpriv->super.lane], &iov, 1,
                            req->send.remote_op.remote_address +
                            req->send.dt_iter.offset,
                            tl_rkey, &req->send.state.uct_comp);
}

static ucs_status_t ucp_proto_rndv_get_zcopy_progress(uct_pending_req_t *self)
{
    return ucp_proto_multi_zcopy_progress(self,
                                          ucp_proto_rndv_get_zcopy_send_func,
                                          ucp_proto_rndv_get_zcopy_completion);
}

static ucs_status_t
ucp_proto_rndv_get_zcopy_init(const ucp_proto_init_params_t *init_params)
{
    ucp_context_t *context               = init_params->worker->context;
    ucp_proto_multi_init_params_t params = {
        .super.super         = *init_params,
        .super.cfg_thresh    = UCS_MEMUNITS_AUTO,
        .super.cfg_priority  = 0,
        .super.flags         = UCP_PROTO_COMMON_INIT_FLAG_SEND_ZCOPY |
                               UCP_PROTO_COMMON_INIT_FLAG_RECV_ZCOPY |
                               UCP_PROTO_COMMON_INIT_FLAG_REMOTE_ACCESS |
                               UCP_PROTO_COMMON_INIT_FLAG_RESPONSE,
        .super.overhead      = 0,
        .super.latency       = 0,
        .max_lanes           = context->config.ext.max_rndv_lanes,
        .first.tl_cap_flags  = UCT_IFACE_FLAG_GET_ZCOPY,
        .super.fragsz_offset = ucs_offsetof(uct_iface_attr_t, cap.get.max_zcopy),
        .first.lane_type     = UCP_LANE_TYPE_RMA_BW,
        .super.hdr_size      = 0,
        .middle.tl_cap_flags = UCT_IFACE_FLAG_GET_ZCOPY,
        .middle.lane_type    = UCP_LANE_TYPE_RMA_BW
    };

    if ((init_params->select_param->op_id != UCP_OP_ID_RNDV_RECV) ||
        (init_params->select_param->dt_class != UCP_DATATYPE_CONTIG) ||
        ((context->config.ext.rndv_mode != UCP_RNDV_MODE_AUTO) &&
         (context->config.ext.rndv_mode != UCP_RNDV_MODE_GET_ZCOPY))) {
        return UCS_ERR_UNSUPPORTED;
    }

    return ucp_proto_multi_init(&params);
}

static ucp_proto_t ucp_rndv_get_zcopy_proto = {
    .name       = "rndv/get/zcopy",
    .flags      = 0,
    .init       = ucp_proto_rndv_get_zcopy_init,
    .config_str = ucp_proto_multi_config_str,
    .progress   = ucp_proto_rndv_get_zcopy_progress
};
UCP_PROTO_REGISTER(&ucp_rndv_get_zcopy_proto);
