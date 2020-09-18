/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "proto_single.h"
#include "proto_common.h"

#include <ucs/debug/assert.h>
#include <ucs/debug/log.h>
#include <ucs/sys/math.h>


ucs_status_t ucp_proto_single_init(const ucp_proto_single_init_params_t *params)
{
    ucp_proto_single_priv_t *spriv = params->super.super.priv;
    ucp_proto_common_perf_params_t perf_params;
    ucp_lane_index_t num_lanes;
    ucp_md_map_t reg_md_map;
    ucp_lane_index_t lane;

    num_lanes = ucp_proto_common_find_lanes(&params->super.super,
                                            params->super.flags,
                                            params->lane_type,
                                            params->tl_cap_flags,
                                            1, 0, &lane, &reg_md_map);
    if (num_lanes == 0) {
        ucs_trace("no lanes for %s", params->super.super.proto_name);
        return UCS_ERR_UNSUPPORTED;
    }

    *params->super.super.priv_size = sizeof(ucp_proto_single_priv_t);

    ucp_proto_common_lane_priv_init(&params->super, reg_md_map, lane,
                                    &spriv->super);

    ucs_assert(ucs_popcount(reg_md_map) <= 1);
    if (reg_md_map == 0) {
        spriv->reg_md      = UCP_NULL_RESOURCE;
    } else {
        spriv->reg_md      = ucs_ffs64(reg_md_map);
    }

    perf_params.lane_map   = UCS_BIT(lane);
    perf_params.reg_md_map = reg_md_map;
    perf_params.lane0      = lane;
    perf_params.is_multi   = 0;
    ucp_proto_common_calc_perf(&params->super, &perf_params);

    return UCS_OK;
}

void ucp_proto_single_config_str(const void *priv, ucs_string_buffer_t *strb)
{
    const ucp_proto_single_priv_t *spriv = priv;

    ucs_string_buffer_init(strb);
    ucp_proto_common_lane_priv_str(&spriv->super, strb);
}
