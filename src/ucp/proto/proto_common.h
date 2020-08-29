/**
 * Copyright (C) Mellanox Technologies Ltd. 2020.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef UCP_PROTO_COMMON_H_
#define UCP_PROTO_COMMON_H_

#include "proto.h"
#include "proto_select.h"


typedef enum {
    UCP_PROTO_COMMON_INIT_FLAG_SEND_ZCOPY    = UCS_BIT(0), /* Send buffer is used by
                                                              zero-copy operations */
    UCP_PROTO_COMMON_INIT_FLAG_RECV_ZCOPY    = UCS_BIT(1), /* Receive side is not
                                                              doing memory copy */
    UCP_PROTO_COMMON_INIT_FLAG_REMOTE_ACCESS = UCS_BIT(2), /* One-sided remote access */
    UCP_PROTO_COMMON_INIT_FLAG_MEM_TYPE      = UCS_BIT(3), /* Protocol supports
                                                              non-host buffers */
    UCP_PROTO_COMMON_INIT_FLAG_RESPONSE      = UCS_BIT(4), /* Sender side has to get
                                                              a reply before can
                                                              complete the request */
    UCP_PROTO_COMMON_INIT_FLAG_HDR_ONLY      = UCS_BIT(5)  /* Header only, no payload */
} ucp_proto_common_init_flags_t;


/* Protocol common initialization parameters which are used to calculate
 * thresholds, performance, etc. */
typedef struct {
    ucp_proto_init_params_t super;
    double                  latency;       /* protocol added latency */
    double                  overhead;      /* protocol overhead */
    size_t                  cfg_thresh;    /* user-configured threshold */
    unsigned                cfg_priority;  /* user configuration priority */
    size_t                  fragsz_offset; /* offset of maximal fragment
                                              size in uct_iface_attr_t */
    size_t                  hdr_size;      /* header size on first lane */
    unsigned                flags;         /* see ucp_proto_common_init_flags_t */
} ucp_proto_common_init_params_t;


typedef struct {
    ucp_lane_map_t          lane_map;      /* Which lanes are used for sending
                                              data in the protocol */
    ucp_md_map_t            reg_md_map;    /* Which memory domains are used for
                                              registration */
    ucp_lane_index_t        lane0;         /* The lane which is used to send the
                                              first fragment, to detect fragment
                                              size and performance ranges */
    int                     is_multi;      /* Whether the protocol can send
                                              multiple fragments */
} ucp_proto_common_perf_params_t;


/* Private data per lane */
typedef struct {
    ucp_lane_index_t        lane;       /* Lane index in the endpoint */
    ucp_rsc_index_t         memh_index; /* Index of UCT memory handle (for zero copy) */
    ucp_md_index_t          rkey_index; /* Remote key index (for remote access) */
} ucp_proto_common_lane_priv_t;


typedef void (*ucp_proto_complete_cb_t)(ucp_request_t *req, ucs_status_t status);


void ucp_proto_common_lane_priv_init(const ucp_proto_common_init_params_t *params,
                                     ucp_md_map_t md_map, ucp_lane_index_t lane,
                                     ucp_proto_common_lane_priv_t *lane_priv);


void ucp_proto_common_lane_priv_str(const ucp_proto_common_lane_priv_t *lpriv,
                                    ucs_string_buffer_t *strb);


ucp_md_index_t
ucp_proto_common_get_md_index(const ucp_proto_init_params_t *params,
                              ucp_lane_index_t lane);


const uct_iface_attr_t *
ucp_proto_common_get_iface_attr(const ucp_proto_init_params_t *params,
                                ucp_lane_index_t lane);


size_t ucp_proto_get_iface_attr_field(const uct_iface_attr_t *iface_attr,
                                      ptrdiff_t field_offset);


double
ucp_proto_common_iface_bandwidth(const ucp_proto_common_init_params_t *params,
                                 const uct_iface_attr_t *iface_attr);


/* @return number of lanes found */
ucp_lane_index_t
ucp_proto_common_find_lanes(const ucp_proto_init_params_t *params, unsigned flags,
                            ucp_lane_type_t lane_type, uint64_t tl_cap_flags,
                            ucp_lane_index_t max_lanes, ucp_lane_map_t exclude_map,
                            ucp_lane_index_t *lanes, ucp_md_map_t *reg_md_map_p);


ucp_lane_index_t
ucp_proto_common_find_am_bcopy_lane(const ucp_proto_init_params_t *params);


void ucp_proto_common_calc_perf(const ucp_proto_common_init_params_t *params,
                                const ucp_proto_common_perf_params_t *perf_params);


void ucp_proto_request_zcopy_completion(uct_completion_t *self, ucs_status_t status);


void ucp_proto_request_select_error(ucp_request_t *req,
                                    ucp_proto_select_t *proto_select,
                                    ucp_worker_cfg_index_t rkey_cfg_index,
                                    const ucp_proto_select_param_t *sel_param,
                                    size_t msg_length);

#endif
