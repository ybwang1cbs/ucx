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


ucs_status_t ucp_proto_rndv_init(const ucp_proto_init_params_t *init_params)
{
    ucp_context_h context                   = init_params->worker->context;
    ucp_proto_remote_op_init_params_t params = {
        .super.super        = *init_params,
        .super.latency      = 0,
        .super.overhead     = 40e-9,
        .super.cfg_thresh   = context->config.ext.rndv_thresh,
        .super.cfg_priority = 60,
        .super.flags        = UCP_PROTO_COMMON_INIT_FLAG_HDR_ONLY |
                              UCP_PROTO_COMMON_INIT_FLAG_RESPONSE,
        .remote_op          = UCP_OP_ID_RNDV_RECV,
        .perf_bias          = context->config.ext.rndv_perf_diff / 100.0
    };

    UCP_RMA_PROTO_INIT_CHECK(init_params, UCP_OP_ID_TAG_SEND);

    return ucp_proto_remote_op_init(&params);
}
