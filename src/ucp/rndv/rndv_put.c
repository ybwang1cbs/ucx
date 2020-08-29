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
#include <ucp/proto/proto_am.inl>
#include <ucp/proto/proto_multi.inl>


typedef struct {
    ucp_proto_multi_priv_t super;
    ucp_lane_index_t       atp_lane;
} ucp_proto_rndv_put_zcopy_priv_t;


static void ucp_proto_rndv_put_zcopy_complete(ucp_request_t *req,
                                              ucs_status_t status)
{
    ucp_trace_req(req, "rndv_put completed");
    ucp_proto_request_zcopy_complete(req, status);
}

static void
ucp_proto_rndv_put_zcopy_completion(uct_completion_t *self, ucs_status_t status)
{
    ucp_request_t *req = ucs_container_of(self, ucp_request_t,
                                          send.state.uct_comp);
    const ucp_proto_rndv_put_zcopy_priv_t *ppriv = req->send.proto_config->priv;
    ucp_reply_hdr_t atp;

    ucp_rkey_destroy(req->send.remote_op.rkey);
    ucp_proto_request_zcopy_cleanup(req);

    // TODO set the header on req->send.tiny_am.payload directly
    atp.req_id = req->send.remote_op.remote_request;
    atp.status = UCS_OK;

    // TODO update protocol state for reconfig
    /* the send request will be completed when ATP is sent */
    ucp_proto_tiny_am_send(req, ppriv->atp_lane, UCP_AM_ID_RNDV_ATP, &atp,
                           sizeof(atp), ucp_proto_rndv_put_zcopy_complete);
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_proto_rndv_put_zcopy_send_func(ucp_request_t *req,
                                   const ucp_proto_multi_lane_priv_t *lpriv,
                                   ucp_datatype_iter_t *next_iter)
{
    ucp_rkey_h    rkey = req->send.remote_op.rkey;
    uct_rkey_t tl_rkey = rkey->tl_rkey[lpriv->super.rkey_index].rkey.rkey;
    uct_iov_t iov;

    ucp_datatype_iter_next_iov(&req->send.dt_iter, lpriv->super.memh_index,
                               ucp_proto_multi_max_payload(req, lpriv, 0),
                               next_iter, &iov);
    return uct_ep_put_zcopy(req->send.ep->uct_eps[lpriv->super.lane], &iov, 1,
                            req->send.remote_op.remote_address +
                            req->send.dt_iter.offset,
                            tl_rkey, &req->send.state.uct_comp);
}

static ucs_status_t ucp_proto_rndv_put_zcopy_progress(uct_pending_req_t *self)
{
    ucp_request_t *req                  = ucs_container_of(self, ucp_request_t,
                                                           send.uct);
    const ucp_proto_multi_priv_t *mpriv = req->send.proto_config->priv;
    ucs_status_t status;

    if (!(req->flags & UCP_REQUEST_FLAG_PROTO_INITIALIZED)) {
        ucp_proto_request_completion_init(req, ucp_proto_rndv_put_zcopy_completion);

        // update memory registration (we could have registered some MDs before
        // when sent RTS, now we can have more/less lanes that we use for
        // PUT operation
        status = ucp_datatype_iter_mem_reg(req->send.ep->worker->context,
                                           &req->send.dt_iter, mpriv->reg_md_map);
        if (status != UCS_OK) {
            // TODO fallback to other protocol
            // TODO send ATP with error?
            ucp_proto_request_zcopy_complete(req, status);
            return UCS_OK;
        }

        ucp_proto_multi_request_init(req);
        req->flags |= UCP_REQUEST_FLAG_PROTO_INITIALIZED;
    }

    return ucp_proto_multi_progress(req, ucp_proto_rndv_put_zcopy_send_func,
                                    ucp_request_invoke_uct_completion,
                                    UCS_BIT(UCP_DATATYPE_CONTIG));
}

static ucs_status_t
ucp_proto_rndv_put_zcopy_init(const ucp_proto_init_params_t *init_params)
{
    ucp_context_t *context                 = init_params->worker->context;
    ucp_proto_rndv_put_zcopy_priv_t *ppriv = init_params->priv;
    ucp_proto_multi_init_params_t params   = {
        .super.super         = *init_params,
        .super.cfg_thresh    = UCS_MEMUNITS_AUTO,
        .super.cfg_priority  = 0,
        .super.flags         = UCP_PROTO_COMMON_INIT_FLAG_SEND_ZCOPY |
                               UCP_PROTO_COMMON_INIT_FLAG_RECV_ZCOPY |
                               UCP_PROTO_COMMON_INIT_FLAG_REMOTE_ACCESS,
        .super.overhead      = 0,
        .super.latency       = 0,
        .max_lanes           = context->config.ext.max_rndv_lanes,
        .first.tl_cap_flags  = UCT_IFACE_FLAG_PUT_ZCOPY,
        .super.fragsz_offset = ucs_offsetof(uct_iface_attr_t, cap.put.max_zcopy),
        .first.lane_type     = UCP_LANE_TYPE_RMA_BW,
        .super.hdr_size      = 0,
        .middle.tl_cap_flags = UCT_IFACE_FLAG_PUT_ZCOPY,
        .middle.lane_type    = UCP_LANE_TYPE_RMA_BW
    };
    ucs_status_t status;

    // TODO support IOV

    if ((init_params->select_param->op_id != UCP_OP_ID_RNDV_SEND) ||
        (init_params->select_param->dt_class != UCP_DATATYPE_CONTIG) ||
        ((context->config.ext.rndv_mode != UCP_RNDV_MODE_AUTO) &&
         (context->config.ext.rndv_mode != UCP_RNDV_MODE_PUT_ZCOPY))) {
        return UCS_ERR_UNSUPPORTED;
    }

    status = ucp_proto_multi_init(&params);
    if (status != UCS_OK) {
        return status;
    }

    ppriv->atp_lane = ucp_proto_common_find_am_bcopy_lane(&params.super.super);
    if (ppriv->atp_lane == UCP_NULL_LANE) {
        return UCS_ERR_UNSUPPORTED;
    }

    return UCS_OK;
}

static void ucp_proto_rndv_put_config_str(const void *priv,
                                          ucs_string_buffer_t *strb)
{
    const ucp_proto_rndv_put_zcopy_priv_t *ppriv = priv;

    ucp_proto_multi_config_str(&ppriv->super, strb);
    ucs_string_buffer_appendf(strb, " atp-ln:%d", ppriv->atp_lane);
}

static ucp_proto_t ucp_rndv_put_zcopy_proto = {
    .name       = "rndv/put/zcopy",
    .flags      = 0,
    .init       = ucp_proto_rndv_put_zcopy_init,
    .config_str = ucp_proto_rndv_put_config_str,
    .progress   = ucp_proto_rndv_put_zcopy_progress
};
UCP_PROTO_REGISTER(&ucp_rndv_put_zcopy_proto);
