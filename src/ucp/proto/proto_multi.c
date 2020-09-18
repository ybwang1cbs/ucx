/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "proto_multi.h"
#include "proto_common.h"
#include "proto_multi.inl"

#include <ucs/debug/assert.h>
#include <ucs/debug/log.h>


ucs_status_t ucp_proto_multi_init(const ucp_proto_multi_init_params_t *params)
{
    ucp_proto_multi_priv_t *mpriv = params->super.super.priv;
    ucp_lane_index_t lanes[UCP_PROTO_MAX_LANES];
    double lanes_bandwidth[UCP_PROTO_MAX_LANES];
    ucp_proto_common_perf_params_t perf_params;
    const uct_iface_attr_t *iface_attr;
    ucp_proto_multi_lane_priv_t *lpriv;
    ucp_md_map_t reg_md_map;
    double total_bandwidth;
    ucp_lane_index_t i;

    ucs_assert(params->max_lanes >= 1);
    ucs_assert(params->max_lanes <= UCP_PROTO_MAX_LANES);

    /* Find first lane */
    mpriv->num_lanes = ucp_proto_common_find_lanes(&params->super.super,
                                                   params->super.flags,
                                                   params->first.lane_type,
                                                   params->first.tl_cap_flags,
                                                   1, 0, lanes, &reg_md_map);
    if (mpriv->num_lanes == 0) {
        ucs_trace("no lanes for %s", params->super.super.proto_name);
        return UCS_ERR_UNSUPPORTED;
    }

    mpriv->reg_md_map = reg_md_map;

    /* Find rest of the lanes */
    mpriv->num_lanes  += ucp_proto_common_find_lanes(&params->super.super,
                                                     params->super.flags,
                                                     params->middle.lane_type,
                                                     params->middle.tl_cap_flags,
                                                     params->max_lanes - 1,
                                                     UCS_BIT(lanes[0]),
                                                     lanes + 1, &reg_md_map);
    mpriv->reg_md_map |= reg_md_map;

    /* Fill the size of private data */
    *params->super.super.priv_size =
            sizeof(ucp_proto_multi_priv_t) +
            (mpriv->num_lanes * ucs_field_sizeof(ucp_proto_multi_priv_t, lanes[0]));

    /* Initialize parameters for calculating performance */
    perf_params.lane_map   = 0;
    perf_params.reg_md_map = mpriv->reg_md_map;
    perf_params.lane0      = lanes[0];
    perf_params.is_multi   = 1;

    /* Collect information from all lanes */
    total_bandwidth = 0;
    for (i = 0; i < mpriv->num_lanes; ++i) {
        lpriv                 = &mpriv->lanes[i];

        perf_params.lane_map |= UCS_BIT(lanes[i]);
        iface_attr            = ucp_proto_common_get_iface_attr(&params->super.super,
                                                                lanes[i]);
        lanes_bandwidth[i]    = ucp_proto_common_iface_bandwidth(&params->super,
                                                                 iface_attr);
        total_bandwidth      += lanes_bandwidth[i];

        lpriv->max_frag       = ucp_proto_get_iface_attr_field(iface_attr,
                                               params->super.fragsz_offset);

        ucp_proto_common_lane_priv_init(&params->super, mpriv->reg_md_map,
                                        lanes[i], &lpriv->super);
    }

    /* Set up the relative weights */
    for (i = 0; i < mpriv->num_lanes; ++i) {
        mpriv->lanes[i].weight = ucs_proto_multi_calc_weight(lanes_bandwidth[i],
                                                             total_bandwidth);
    }

    ucp_proto_common_calc_perf(&params->super, &perf_params);

    return UCS_OK;
}

void ucp_proto_multi_config_str(const void *priv, ucs_string_buffer_t *strb)
{
    const ucp_proto_multi_priv_t *mpriv = priv;
    const ucp_proto_multi_lane_priv_t *lpriv;
    size_t percent, remaining;
    ucp_lane_index_t i;

    remaining = 100;
    ucs_string_buffer_init(strb);
    for (i = 0; i < mpriv->num_lanes; ++i) {
        if (i > 0) {
            ucs_string_buffer_appendf(strb, " ");
        }

        lpriv      = &mpriv->lanes[i];
        percent    = ucs_min(remaining,
                             ucp_proto_multi_scaled_length(lpriv, 100));
        remaining -= percent;

        ucs_string_buffer_appendf(strb, "%zu%% ", percent);
        ucp_proto_common_lane_priv_str(&lpriv->super, strb);
    }
}
