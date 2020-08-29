/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "proto_rndv.h"

#include <ucp/proto/proto_remote_op.h>
#include <ucp/proto/proto_am.inl>


ucs_status_t ucp_proto_rndv_rtr_handle_atp(void *arg, void *data, size_t length,
                                            unsigned flags)
{
    ucp_worker_h worker     = arg;
    ucp_reply_hdr_t *atp    = data;
    ucp_request_t *rtr_req  = ucp_worker_get_request_by_id(worker, atp->req_id);
    ucp_request_t *recv_req = rtr_req->send.remote_op.rndv_recv.recv_req;
    ucs_status_t status     = atp->status;

    // TODO check status
    ucs_assert(length == sizeof(*atp));

    // TODO common cleanup for remote_op proto request
    // TODO we should keep the rkey only if the protocol selected by remote_op
    // handler actually uses it
    ucp_proto_request_zcopy_cleanup(rtr_req);
    ucp_rkey_destroy(rtr_req->send.remote_op.rkey);
    ucp_request_put(rtr_req);

    ucp_request_complete_tag_recv(recv_req, status);
    return UCS_OK;
}

static size_t ucp_proto_rndv_rtr_pack(void *dest, void *arg)
{
    ucp_rndv_rtr_hdr_t *rndv_rtr_hdr = dest;
    ucp_request_t *req               = arg;
    size_t packed_rkey_size;

    rndv_rtr_hdr->sreq_id  = req->send.remote_op.remote_request;
    // NOTE: we send the pointer to RTR send request and not the receive request
    // TODO send rdnv_get from recv request instead of allocating another request
    // on receiver side
    rndv_rtr_hdr->rreq_id = ucp_send_request_get_id(req);
    rndv_rtr_hdr->size    = req->send.dt_iter.length;
    rndv_rtr_hdr->offset  = 0;

    packed_rkey_size = ucp_proto_remote_op_pack_am(req, &rndv_rtr_hdr->address,
                                                  rndv_rtr_hdr + 1);
    return sizeof(*rndv_rtr_hdr) + packed_rkey_size;
}

static ucs_status_t ucp_proto_rndv_rtr_progress(uct_pending_req_t *self)
{
    ucp_request_t *req                     = ucs_container_of(self, ucp_request_t,
                                                              send.uct);
    const ucp_proto_remote_op_priv_t *kpriv = req->send.proto_config->priv;
    size_t max_rtr_size                    = sizeof(ucp_rndv_rtr_hdr_t) +
                                             kpriv->packed_rkey_size;
    ucs_status_t status;

    if (!(req->flags & UCP_REQUEST_FLAG_PROTO_INITIALIZED)) {
        status = ucp_proto_request_zcopy_init(req, kpriv->md_map, NULL);
        if (status != UCS_OK) {
            // TODO send message with error indication?
            ucs_warn("RTR failed to register, not sending keys");
            return UCS_OK;
        }

        req->flags |= UCP_REQUEST_FLAG_PROTO_INITIALIZED;
    }

    // TODO fragment RTR - send multiple messages for portions of the receive
    // buffer
    // the request will be completed by ATP message from remote side
    return ucp_proto_am_bcopy_single_progress(req, UCP_AM_ID_RNDV_RTR,
                                              kpriv->lane,
                                              ucp_proto_rndv_rtr_pack,
                                              max_rtr_size, NULL);
}

static ucs_status_t
ucp_proto_rndv_rtr_init(const ucp_proto_init_params_t *init_params)
{
    ucp_proto_remote_op_init_params_t params = {
        .super.super        = *init_params,
        .super.latency      = 0,
        .super.overhead     = 40e-9,
        .super.cfg_thresh   = UCS_MEMUNITS_AUTO,
        .super.cfg_priority = 0,
        .super.flags        = UCP_PROTO_COMMON_INIT_FLAG_HDR_ONLY |
                              UCP_PROTO_COMMON_INIT_FLAG_RESPONSE,
        .remote_op          = UCP_OP_ID_RNDV_SEND,
        .perf_bias          = 0.0
    };

    UCP_RMA_PROTO_INIT_CHECK(init_params, UCP_OP_ID_RNDV_RECV);

    return ucp_proto_remote_op_init(&params);
}

static ucp_proto_t ucp_rndv_rtr_proto = {
    .name       = "rndv/rtr",
    .flags      = 0,
    .init       = ucp_proto_rndv_rtr_init,
    .config_str = ucp_proto_remote_op_config_str,
    .progress   = ucp_proto_rndv_rtr_progress
};
UCP_PROTO_REGISTER(&ucp_rndv_rtr_proto);
