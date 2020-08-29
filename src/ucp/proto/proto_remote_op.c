/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "proto_remote_op.h"

#include <ucp/core/ucp_rkey.inl>
#include <ucp/proto/proto_am.inl>


size_t ucp_proto_remote_op_pack_am(ucp_request_t *req, uintptr_t *address_p,
                                  void *rkey_buffer)
{
    const ucp_proto_remote_op_priv_t UCS_V_UNUSED *rpriv =
                                            req->send.proto_config->priv;
    ssize_t packed_rkey_size;

    /* For contiguous buffer, pack one rkey
     * TODO instead of putting address in RTS, write N [address+length] records,
     *      to support IOV datatype
     */
    ucs_assert(req->send.dt_iter.dt_class == UCP_DATATYPE_CONTIG);
    ucs_assert(req->send.dt_iter.type.contig.reg.md_map != 0);

    *address_p       = (uintptr_t)req->send.dt_iter.type.contig.buffer;
    packed_rkey_size = ucp_rkey_pack_uct(req->send.ep->worker->context,
                                         req->send.dt_iter.type.contig.reg.md_map,
                                         req->send.dt_iter.type.contig.reg.memh,
                                         req->send.dt_iter.mem_type,
                                         rkey_buffer);
    if (packed_rkey_size < 0) {
        ucs_error("failed to pack RTS remote key: %s",
                  ucs_status_string((ucs_status_t)packed_rkey_size));
        return 0;
    }

    ucs_assert(packed_rkey_size == rpriv->packed_rkey_size);
    return packed_rkey_size;
}

ucs_status_t
ucp_proto_remote_op_send_reply(ucp_worker_h worker, ucp_request_t *req,
                              ucp_operation_id_t op_id, uint8_t sg_count,
                              const void *rkey_buffer)
{
    ucp_proto_select_param_t sel_param;
    ucs_status_t status;
    ucp_rkey_h rkey;

    status = ucp_ep_rkey_unpack(req->send.ep, rkey_buffer, &rkey);
    if (status != UCS_OK) {
        goto err;
    }

    ucp_proto_select_param_init(&sel_param, op_id, 0, req->send.dt_iter.dt_class,
                                req->send.dt_iter.mem_type, sg_count);

    status = ucp_proto_request_set_proto(worker, req->send.ep, req,
                                         &ucp_rkey_config(worker, rkey)->proto_select,
                                         rkey->cfg_index, &sel_param,
                                         req->send.dt_iter.length);
    if (status != UCS_OK) {
        goto err_destroy_rkey;
    }

    req->send.remote_op.rkey = rkey;

    ucp_request_send(req, 0);
    return UCS_OK;

err_destroy_rkey:
    ucp_rkey_destroy(rkey);
err:
    return status;
}

static ucp_md_map_t
ucp_proto_remote_op_reg_md_map(const ucp_proto_remote_op_init_params_t *params)
{
    ucp_worker_h                      worker = params->super.super.worker;
    const ucp_ep_config_key_t *ep_config_key = params->super.super.ep_config_key;
    ucs_memory_type_t               mem_type = params->super.super.select_param->mem_type;
    const uct_iface_attr_t *iface_attr;
    const uct_md_attr_t *md_attr;
    ucp_md_index_t md_index;
    ucp_md_map_t reg_md_map;
    ucp_lane_index_t lane;

    /* md_map is all lanes which support get_zcopy on the given mem_type and
     * require remote key
     * TODO register only on devices close to the memory
     */
    reg_md_map = 0;
    for (lane = 0; lane < ep_config_key->num_lanes; ++lane) {
        if (ep_config_key->lanes[lane].rsc_index == UCP_NULL_RESOURCE) {
            continue;
        }

        /* Check the lane supports get_zcopy */
        // TODO improve the accuracy of selecting initial MDs
        iface_attr = ucp_proto_common_get_iface_attr(&params->super.super, lane);
        if (!(iface_attr->cap.flags & (UCT_IFACE_FLAG_GET_ZCOPY |
                                       UCT_IFACE_FLAG_PUT_ZCOPY))) {
            continue;
        }

        /* Check the memory domain requires remote key, and capable of
         * registering the memory type
         */

        md_index = ucp_proto_common_get_md_index(&params->super.super, lane);
        md_attr  = &worker->context->tl_mds[md_index].attr;
        if (!(md_attr->cap.flags & UCT_MD_FLAG_NEED_RKEY) ||
            !(md_attr->cap.reg_mem_types & UCS_BIT(mem_type))) {
            continue;
        }

        reg_md_map |= UCS_BIT(md_index);
    }

    return reg_md_map;
}

static ucs_status_t
ucp_proto_remote_op_select_remote(const ucp_proto_remote_op_init_params_t *params,
                                 const ucp_proto_select_param_t *remote_select_param,
                                 ucp_proto_remote_op_priv_t *rpriv)
{
    ucp_worker_h                 worker = params->super.super.worker;
    ucp_worker_cfg_index_t ep_cfg_index = params->super.super.ep_cfg_index;
    ucp_rkey_config_key_t rkey_config_key;
    ucp_worker_cfg_index_t rkey_cfg_index;
    ucp_proto_select_elem_t *select_elem;
    ucp_rkey_config_t *rkey_config;
    ucs_status_t status;

    /* Construct remote key for remote protocol lookup according to the local
     * buffer properties (since remote side is expected to access the local
     * buffer)
     */
    rkey_config_key.md_map       = rpriv->md_map;
    rkey_config_key.ep_cfg_index = ep_cfg_index;
    rkey_config_key.mem_type     = params->super.super.select_param->mem_type;
    rkey_config_key.sys_dev      = params->super.super.select_param->sys_dev;

    status = ucp_worker_get_rkey_config(worker, &rkey_config_key,
                                        &rkey_cfg_index);
    if (status != UCS_OK) {
        return status;
    }

    rkey_config = &worker->rkey_config[rkey_cfg_index];
    select_elem = ucp_proto_select_lookup_slow(worker, &rkey_config->proto_select,
                                               ep_cfg_index, rkey_cfg_index,
                                               remote_select_param);
    if (select_elem == NULL) {
        ucs_debug("%s: did not find protocol for %s",
                  params->super.super.proto_name,
                  ucp_operation_names[params->remote_op]);
        return UCS_ERR_UNSUPPORTED;
    }

    rpriv->remote_proto = *select_elem;
    return UCS_OK;
}

ucs_status_t
ucp_proto_remote_op_init(const ucp_proto_remote_op_init_params_t *params)
{
    ucp_context_h            context = params->super.super.worker->context;
    ucp_proto_remote_op_priv_t *rpriv = params->super.super.priv;
    const ucp_proto_perf_range_t *remote_perf_range;
    ucp_proto_select_param_t remote_select_param;
    ucp_proto_perf_range_t *perf_range;
    const uct_iface_attr_t *iface_attr;
    ucs_linear_func_t send_overheads;
    ucp_md_index_t md_index;
    ucp_proto_caps_t *caps;
    ucs_status_t status;
    double rts_latency;

    ucs_assert(params->super.flags == (UCP_PROTO_COMMON_INIT_FLAG_HDR_ONLY |
                                       UCP_PROTO_COMMON_INIT_FLAG_RESPONSE));

    /* Find lane to send the initial message */
    rpriv->lane = ucp_proto_common_find_am_bcopy_lane(&params->super.super);
    if (rpriv->lane == UCP_NULL_LANE) {
        return UCS_ERR_UNSUPPORTED;
    }

    /* Initialize estimated memory registration map */
    rpriv->md_map           = ucp_proto_remote_op_reg_md_map(params);
    rpriv->packed_rkey_size = ucp_rkey_packed_size(context, rpriv->md_map);

    /* Construct select parameter for the remote protocol */
    if (params->super.super.rkey_config_key == NULL) {
        /* Remote buffer is unknown, assume same params as local */
        ucp_proto_select_param_init(&remote_select_param, params->remote_op, 0,
                                    params->super.super.select_param->dt_class,
                                    params->super.super.select_param->mem_type,
                                    params->super.super.select_param->sg_count);
    } else {
        /* If we know the remote buffer parameters, these are actually the local
         * parameters for the remote protocol
         */
        ucp_proto_select_param_init(&remote_select_param, params->remote_op, 0,
                                    UCP_DATATYPE_CONTIG /* TODO add dtype to rkey */,
                                    params->super.super.rkey_config_key->mem_type,
                                    1 /* TODO add sg_count to rkey */);
    }

    status = ucp_proto_remote_op_select_remote(params, &remote_select_param,
                                              rpriv);
    if (status != UCS_OK) {
        return status;
    }

    /* Set send_overheads to the time to send and receive RTS message */
    // TODO take into account packed rkey size overhead
    iface_attr     = ucp_proto_common_get_iface_attr(&params->super.super,
                                                     rpriv->lane);
    rts_latency    = (iface_attr->overhead * 2) +
                     ucp_tl_iface_latency(context, &iface_attr->latency);
    send_overheads = ucs_linear_func_make(rts_latency, 0.0);

    /* Add registration cost to send_overheads */
    ucs_for_each_bit(md_index, rpriv->md_map) {
        ucs_linear_func_add_inplace(&send_overheads,
                                    context->tl_mds[md_index].attr.reg_cost);
    }

    /* Initialize protocol configuration space */
    *params->super.super.priv_size         = sizeof(ucp_proto_remote_op_priv_t);
    params->super.super.caps->cfg_thresh   = params->super.cfg_thresh;
    params->super.super.caps->cfg_priority = params->super.cfg_priority;
    params->super.super.caps->min_length   = 0;
    params->super.super.caps->num_ranges   = 0;

    /* Copy performance ranges from the remote protocol and add overheads */
    remote_perf_range = rpriv->remote_proto.perf_ranges;
    caps              = params->super.super.caps;
    do {
        perf_range             = &caps->ranges[caps->num_ranges];
        perf_range->max_length = remote_perf_range->max_length;

        /* add send overheads and scale by 1-perf_bias */
        perf_range->perf = ucs_linear_func_compose(
                    ucs_linear_func_make(0, 1.0 - params->perf_bias),
                    ucs_linear_func_add(remote_perf_range->perf, send_overheads));

        ++caps->num_ranges;
    } while ((remote_perf_range++)->max_length != SIZE_MAX);

    return UCS_OK;
}

void ucp_proto_remote_op_config_str(const void *priv, ucs_string_buffer_t *strb)
{
    const ucp_proto_remote_op_priv_t *rpriv = priv;
    const ucp_proto_threshold_elem_t *thresh_elem;
    const ucp_proto_t *proto;
    ucp_md_index_t md_index;
    ucs_string_buffer_t sb;
    char str[64];

    ucs_string_buffer_init(strb);
    ucs_string_buffer_appendf(strb, "am-ln:%d md:", rpriv->lane);
    ucs_for_each_bit(md_index, rpriv->md_map) {
        ucs_string_buffer_appendf(strb, "%d,", md_index);
    }
    ucs_string_buffer_rtrim(strb, ",");

    ucs_string_buffer_appendf(strb, " ");

    thresh_elem = rpriv->remote_proto.thresholds;
    do {
        // TODO add "short to_string" for proto config
        proto = thresh_elem->proto_config.proto;
        proto->config_str(thresh_elem->proto_config.priv, &sb);
        ucs_string_buffer_appendf(strb, "%s(%s)", proto->name,
                                  ucs_string_buffer_cstr(&sb));
        ucs_string_buffer_cleanup(&sb);

        if (thresh_elem->max_msg_length != SIZE_MAX) {
            ucs_memunits_to_str(thresh_elem->max_msg_length, str, sizeof(str));
            ucs_string_buffer_appendf(strb, "<=%s<", str);
        }
    } while ((thresh_elem++)->max_msg_length != SIZE_MAX);

    ucs_string_buffer_rtrim(strb, "<");
}
