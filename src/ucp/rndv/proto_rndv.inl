/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCP_PROTO_RNDV_INL_
#define UCP_PROTO_RNDV_INL_

#include "proto_rndv.h"

#include <ucp/proto/proto_remote_op.h>
#include <ucp/proto/proto_am.inl>
#include <ucp/proto/proto_multi.inl>


static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_proto_rndv_rts_request_init(ucp_request_t *req)
{
    const ucp_proto_remote_op_priv_t *kpriv = req->send.proto_config->priv;
    ucs_status_t status;

    if (req->flags & UCP_REQUEST_FLAG_PROTO_INITIALIZED) {
        return UCS_OK;
    }

    status = ucp_ep_resolve_remote_id(req->send.ep, kpriv->lane);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_proto_request_zcopy_init(req, kpriv->md_map,
                                          ucp_proto_request_zcopy_completion);
    if (status != UCS_OK) {
        // TODO fallback to other protocol
        ucp_proto_request_zcopy_complete(req, status);
        return status;
    }

    req->flags |= UCP_REQUEST_FLAG_PROTO_INITIALIZED;
    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE size_t
ucp_proto_rndv_rts_pack(ucp_request_t *req, void *dest)
{
    ucp_rndv_rts_hdr_t *rndv_rts_hdr = dest;
    size_t packed_rkey_size;

    rndv_rts_hdr->sreq.req_id = ucp_send_request_get_id(req);
    rndv_rts_hdr->sreq.ep_id  = ucp_send_request_get_ep_remote_id(req);
    rndv_rts_hdr->size        = req->send.dt_iter.length;
    rndv_rts_hdr->flags       = UCP_RNDV_RTS_FLAG_TAG;

    packed_rkey_size = ucp_proto_remote_op_pack_am(req, &rndv_rts_hdr->address,
                                                   rndv_rts_hdr + 1);
    return sizeof(*rndv_rts_hdr) + packed_rkey_size;
}

static UCS_F_ALWAYS_INLINE void
ucp_proto_rndv_receive(ucp_worker_h worker, ucp_request_t *recv_req,
                       const ucp_rndv_rts_hdr_t *rndv_rts_hdr)
{
    ucs_status_t status;
    ucp_request_t *req;
    uint8_t sg_count;

    req = ucp_request_get(worker);
    if (req == NULL) {
        ucs_error("failed to allocate rendezvous reply");
        return;
    }

    req->flags   = 0;
    req->send.ep = ucp_worker_get_ep_by_id(worker, rndv_rts_hdr->sreq.ep_id);

    // TODO avoid re-detection of recv buffer
    // TODO recv req should have dt_iter as well, and we can copy the data
    // directly from it
    // TODO reorganize request structure so that receive request itself can
    // be reused for rndv operation (move callback and iter to common part)
    ucp_datatype_iter_init(worker->context, recv_req->recv.buffer,
                           recv_req->recv.count, recv_req->recv.datatype,
                           recv_req->recv.length,
                           &req->send.dt_iter, &sg_count);

    // TODO need to register memory in the receive request since RTR goes away
    // too quickly

    req->send.remote_op.remote_address     = rndv_rts_hdr->address;
    req->send.remote_op.remote_request     = rndv_rts_hdr->sreq.req_id;
    req->send.remote_op.rndv_recv.recv_req = recv_req;

    // TODO add common wire header for "remote_op_proto"
    status = ucp_proto_remote_op_send_reply(worker, req, UCP_OP_ID_RNDV_RECV,
                                           sg_count, rndv_rts_hdr + 1);
    if (status != UCS_OK) {
        // TODO send error response?
        ucp_request_put(req);
    }
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucp_proto_rndv_handle_rtr(void *arg, void *data, size_t length, unsigned flags)
{
    ucp_worker_h              worker = arg;
    ucp_rndv_rtr_hdr_t *rndv_rtr_hdr = data;
    ucs_status_t status;
    ucp_request_t *req;

    // TODO better handling of switching request from RTS to RNDV_SEND
    // maybe have request init func for a protocol?
//    ucs_assert(!(req->flags & UCP_REQUEST_FLAG_PROTO_INITIALIZED));

    req = ucp_worker_get_request_by_id(worker, rndv_rtr_hdr->sreq_id);

    req->flags                        &= ~UCP_REQUEST_FLAG_PROTO_INITIALIZED;
    req->send.remote_op.remote_address = rndv_rtr_hdr->address;
    req->send.remote_op.remote_request = rndv_rtr_hdr->rreq_id;

    // select a protocol to do RNDV_SEND
    status = ucp_proto_remote_op_send_reply(worker, req, UCP_OP_ID_RNDV_SEND,
                                           1 /* TODO */, rndv_rtr_hdr + 1);
    if (status != UCS_OK) {
        ucp_proto_request_zcopy_complete(req, status);
    }

    return UCS_OK;
}

#endif
