/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCP_PROTO_KICKBACK_H_
#define UCP_PROTO_KICKBACK_H_

#include "proto_common.h"
#include "proto_select.h"
#include "proto_am.h"


/*
 * Protocol which sends back message(s), potentially with remote access keys, to
 * the remote peer - instead of actually initiating data transfer on the local
 * side.
 */
typedef struct {
    ucp_md_map_t            md_map;      /* Memory domains to send remote keys */
    size_t                  packed_rkey_size; /* Total size of packed rkeys */
    ucp_lane_index_t        lane;             /* Lane for sending the "remote_op" message */
    ucp_proto_select_elem_t remote_proto;     /* Which protocol the remote side
                                                 is expected to use, for performance
                                                 estimation and reporting purpose */
} ucp_proto_remote_op_priv_t;


typedef struct {
    ucp_proto_common_init_params_t  super;
    ucp_operation_id_t              remote_op;
    double                          perf_bias;
} ucp_proto_remote_op_init_params_t;


size_t ucp_proto_remote_op_pack_am(ucp_request_t *req, uintptr_t *address_p,
                                  void *rkey_buffer);

ucs_status_t
ucp_proto_remote_op_send_reply(ucp_worker_h worker, ucp_request_t *req,
                              ucp_operation_id_t op_id, uint8_t sg_count,
                              const void *rkey_buffer);

ucs_status_t
ucp_proto_remote_op_init(const ucp_proto_remote_op_init_params_t *params);


void ucp_proto_remote_op_config_str(const void *priv, ucs_string_buffer_t *strb);


#endif
