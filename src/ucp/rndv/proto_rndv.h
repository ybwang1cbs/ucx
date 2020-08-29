/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCP_PROTO_RNDV_H_
#define UCP_PROTO_RNDV_H_

#include "rndv.h"

ucs_status_t ucp_proto_rndv_init(const ucp_proto_init_params_t *init_params);

ucs_status_t ucp_proto_rndv_rtr_handle_atp(void *arg, void *data, size_t length,
                                           unsigned flags);

#endif
