// SPDX-License-Identifier: GPL-2.0-only
/*
 * NAMRBD kernel control plane.
 *
 * This module registers the NAMRBD_CTRL generic-netlink family used by
 * namrbdctl and related host-side tools. It is the local control bridge between
 * userspace/gateway attachment state and namrbd_blk.ko.
 *
 * The control module handles:
 * - create/destroy of local block devices
 * - configuration of per-device gateway REST control endpoints
 * - userspace-provided attach manifests and the compatibility kernel-mediated
 *   attach/info/detach REST path
 * - manifest parsing and validation for volume, attachment, host/device,
 *   geometry, dataplane endpoint, limits, and optional dataplane auth fields
 * - dataplane reconfiguration, path-plan mask application, local detach, resize
 * - status/list replies that expose block module path, lane, no-path, and
 *   endpoint state
 *
 * It does not implement blk-mq dispatch, complete block requests, own storage
 * placement, or decide attachment authority. It validates and applies
 * gateway/control-metadata results to the block module.
 *
 * The in-kernel REST compatibility path remains intentionally narrow. The
 * normal host workflow may fetch gateway manifests in userspace and submit them
 * through generic netlink; foreground block I/O uses the block module's binary
 * gateway dataplane, not this HTTP control path.
 */

#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/types.h>
#include <linux/vmalloc.h>

#include <net/genetlink.h>
#include <net/sock.h>

#include "namrbd_transport.h"
#include "../uapi/namrbd_netlink.h"

#define NAMRBD_MAX_ADDR_LEN 64
#define NAMRBD_MAX_PREFIX_LEN 128
#define NAMRBD_MAX_TOKEN_LEN 256
#define NAMRBD_MAX_HOST_ID_LEN 128
#define NAMRBD_MAX_HTTP_RESP 8192
#define NAMRBD_MAX_JSON_STR 128
#define NAMRBD_MAX_LIST_DEVICES 256
#define NAMRBD_MAX_AUTH_MODE 32
#define NAMRBD_MAX_DATAPLANE_TOKEN 2048
#define NAMRBD_MAX_DATAPLANE_SESSION_KEY 256

struct namrbd_attach_manifest {
	u64 volume_id;
	u64 generation;
	u64 size_bytes;
	u64 max_inflight_bytes;
	u32 max_inflight_requests;
	u32 max_io_size;
	u32 max_zero_like_io_size;
	u32 block_size;
	u32 chunk_size_bytes;
	u32 attached_device_id;
	u32 dataplane_path_count;
	bool initial_zero_map_all_zero;
	char attachment_id[NAMRBD_MAX_JSON_STR];
	char attached_host_id[NAMRBD_MAX_HOST_ID_LEN];
	struct namrbd_transport_path dataplane_paths[NAMRBD_TRANSPORT_MAX_PATHS];
	/* Phase C3: dataplane auth (wire v2) */
	char dataplane_auth_mode[NAMRBD_MAX_AUTH_MODE];
	char dataplane_token[NAMRBD_MAX_DATAPLANE_TOKEN];
	char dataplane_session_key[NAMRBD_MAX_DATAPLANE_SESSION_KEY];
};

struct namrbd_blk_status {
	u32 device_id;
	char disk_name[DISK_NAME_LEN];
	u8 attached;
	u64 volume_id;
	u64 generation;
	u32 path_count;
	u64 down_mask;
	u64 degraded_mask;
	u64 draining_mask;
	u64 applied_path_plan_revision;
	u32 active_lane_count;
	u32 nr_hw_queues;
	u32 target_nr_hw_queues;
	u64 queue_topology_generation;
	char queue_topology_state[24];
	u64 lane_remap_count;
	u32 last_lane_remapped_lanes;
	u64 last_lane_remap_jiffies;
	char last_lane_remap_reason[32];
	u32 no_path_retry_mode;
	u32 no_path_retry_seconds;
	u32 no_path_state;
	u64 no_path_since_jiffies;
	u64 no_path_retry_deadline_jiffies;
	u64 last_no_path_wakeup_jiffies;
	u64 no_path_queued_reqs;
	u64 no_path_requeued_reqs;
	u64 no_path_failed_reqs;
	u64 no_path_recovered_reqs;
	u64 no_path_enter_count;
	u32 last_no_path_reason;
	u32 last_no_path_op;
	u32 last_no_path_eligible_paths;
	u64 last_no_path_tried_mask;
	u64 last_no_path_jiffies;
	struct {
		u32 path_id;
		u32 state;
		u32 consecutive_errors;
		u32 last_errno;
		u32 last_wire_status;
		u32 priority;
		u8 connected;
		u32 inflight;
		u32 pending;
		u32 pending_high_water;
		u32 outstanding_limit;
		u64 submitted;
		u64 completed;
		u64 retries;
		u64 conn_opens;
		u64 conn_resets;
		u16 port;
		u8 use_tls;
		char gateway_id[NAMRBD_TRANSPORT_GATEWAY_ID_LEN];
		char address[NAMRBD_TRANSPORT_ADDR_LEN];
		char server_name[NAMRBD_TRANSPORT_SERVER_NAME_LEN];
	} paths[16];
	struct {
		u32 lane_id;
		u32 preferred_path_id;
		u32 fallback_path_id;
		u32 readiness;
		u64 dispatch_reqs;
	} lanes[16];
};

extern int namrbd_blk_create(u32 *device_id_out);
extern int namrbd_blk_destroy(u32 device_id);
extern int namrbd_blk_activate_device(u32 device_id, u64 volume_id, u64 size_bytes,
				      u32 block_size, u32 chunk_size_bytes,
				      u64 generation);
extern int namrbd_blk_activate_device_with_initial_zero_map(u32 device_id, u64 volume_id,
							    u64 size_bytes, u32 block_size,
							    u32 chunk_size_bytes,
							    u64 generation,
							    bool initial_zero_map_all_zero);
extern int namrbd_blk_resize_device(u32 device_id, u64 volume_id, u64 generation,
				    u64 size_bytes);
extern void namrbd_blk_deactivate_device(u32 device_id, u64 volume_id);
extern int namrbd_blk_configure_data_paths_device(u32 device_id,
						  const struct namrbd_transport_path *paths,
						  u32 dataplane_path_count,
						  u32 max_inflight_requests,
						  u64 max_inflight_bytes,
						  u32 max_io_size,
						  u32 max_zero_like_io_size,
						  const char *host_id,
						  const char *dataplane_auth_mode,
						  const char *dataplane_token,
						  const char *dataplane_session_key);
extern int namrbd_blk_configure_data_path_device(u32 device_id, const char *address, u16 port,
						 u32 dataplane_path_count,
						 u32 max_inflight_requests,
						 u64 max_inflight_bytes,
						 u32 max_io_size,
						 u32 max_zero_like_io_size,
						 const char *host_id,
						 const char *dataplane_auth_mode,
						 const char *dataplane_token,
						 const char *dataplane_session_key);
extern int namrbd_blk_get_status(u32 device_id, struct namrbd_blk_status *out);
extern int namrbd_blk_list_devices(struct namrbd_blk_status *out, u32 max_entries,
				   u32 *count_out);
extern int namrbd_blk_update_path_masks_device(u32 device_id, u64 path_plan_revision,
					       u64 down_mask_bits, u64 degraded_mask_bits,
					       u64 draining_mask_bits);

struct namrbd_rest_server {
	struct list_head list;
	u32 id;
	char address[NAMRBD_MAX_ADDR_LEN];
	u16 port;
	bool use_tls;
	char api_prefix[NAMRBD_MAX_PREFIX_LEN];
	char bearer_token[NAMRBD_MAX_TOKEN_LEN];
};

struct namrbd_device_ctx {
	struct list_head list;
	u32 device_id;
	struct list_head servers;
	char attachment_id[NAMRBD_MAX_JSON_STR];
};

static LIST_HEAD(namrbd_device_ctxs);
static DEFINE_MUTEX(namrbd_device_ctxs_lock);
static struct genl_family namrbd_genl_family;

static struct nla_policy namrbd_policy[NAMRBD_ATTR_MAX + 1] = {
	[NAMRBD_ATTR_DEVICE_ID] = { .type = NLA_U32 },
	[NAMRBD_ATTR_SERVERS] = { .type = NLA_NESTED },
	[NAMRBD_ATTR_ATTACH_REQ] = { .type = NLA_NESTED },
	[NAMRBD_ATTR_DETACH_REQ] = { .type = NLA_NESTED },
	[NAMRBD_ATTR_MANIFEST_JSON] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_HTTP_RESP - 1 },
	[NAMRBD_ATTR_DOWN_MASK] = { .type = NLA_U64 },
	[NAMRBD_ATTR_DEGRADED_MASK] = { .type = NLA_U64 },
	[NAMRBD_ATTR_DRAINING_MASK] = { .type = NLA_U64 },
	[NAMRBD_ATTR_PATH_PLAN_REVISION] = { .type = NLA_U64 },
	[NAMRBD_ATTR_SIZE_BYTES] = { .type = NLA_U64 },
	[NAMRBD_ATTR_VOLUME_ID] = { .type = NLA_U64 },
	[NAMRBD_ATTR_GENERATION] = { .type = NLA_U64 },
};

static struct nla_policy namrbd_server_policy[NAMRBD_SERVER_ATTR_MAX + 1] = {
	[NAMRBD_SERVER_ATTR_ID] = { .type = NLA_U32 },
	[NAMRBD_SERVER_ATTR_ADDRESS] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_ADDR_LEN - 1 },
	[NAMRBD_SERVER_ATTR_PORT] = { .type = NLA_U16 },
	[NAMRBD_SERVER_ATTR_USE_TLS] = { .type = NLA_U8 },
	[NAMRBD_SERVER_ATTR_API_PREFIX] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_PREFIX_LEN - 1 },
	[NAMRBD_SERVER_ATTR_BEARER_TOKEN] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_TOKEN_LEN - 1 },
};

static struct nla_policy namrbd_req_policy[NAMRBD_REQ_ATTR_MAX + 1] = {
	[NAMRBD_REQ_ATTR_DEVICE_ID] = { .type = NLA_U32 },
	[NAMRBD_REQ_ATTR_HOST_ID] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_HOST_ID_LEN - 1 },
	[NAMRBD_REQ_ATTR_VOLUME_ID] = { .type = NLA_U64 },
};

static struct nla_policy namrbd_attach_manifest_req_policy[NAMRBD_ATTR_MAX + 1] = {
	[NAMRBD_REQ_ATTR_DEVICE_ID] = { .type = NLA_U32 },
	[NAMRBD_REQ_ATTR_HOST_ID] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_HOST_ID_LEN - 1 },
	[NAMRBD_REQ_ATTR_VOLUME_ID] = { .type = NLA_U64 },
	[NAMRBD_ATTR_MANIFEST_JSON] = { .type = NLA_NUL_STRING, .len = NAMRBD_MAX_HTTP_RESP - 1 },
};

static void namrbd_free_servers(struct list_head *servers)
{
	struct namrbd_rest_server *s, *tmp;

	list_for_each_entry_safe(s, tmp, servers, list) {
		list_del(&s->list);
		kfree(s);
	}
}

static struct namrbd_device_ctx *namrbd_find_device_ctx_locked(u32 device_id)
{
	struct namrbd_device_ctx *ctx;

	list_for_each_entry(ctx, &namrbd_device_ctxs, list) {
		if (ctx->device_id == device_id)
			return ctx;
	}
	return NULL;
}

static struct namrbd_device_ctx *namrbd_get_or_create_device_ctx_locked(u32 device_id)
{
	struct namrbd_device_ctx *ctx;

	ctx = namrbd_find_device_ctx_locked(device_id);
	if (ctx)
		return ctx;

	ctx = kzalloc(sizeof(*ctx), GFP_KERNEL);
	if (!ctx)
		return NULL;
	ctx->device_id = device_id;
	INIT_LIST_HEAD(&ctx->servers);
	list_add_tail(&ctx->list, &namrbd_device_ctxs);
	return ctx;
}

static void namrbd_remove_device_ctx_locked(u32 device_id)
{
	struct namrbd_device_ctx *ctx;

	ctx = namrbd_find_device_ctx_locked(device_id);
	if (!ctx)
		return;
	list_del(&ctx->list);
	namrbd_free_servers(&ctx->servers);
	kfree(ctx);
}

static struct namrbd_rest_server *namrbd_first_server_locked(struct namrbd_device_ctx *ctx)
{
	if (!ctx || list_empty(&ctx->servers))
		return NULL;
	return list_first_entry(&ctx->servers, struct namrbd_rest_server, list);
}

static const char *namrbd_find_http_body(char *resp)
{
	char *p = strstr(resp, "\r\n\r\n");

	if (!p)
		return NULL;
	return p + 4;
}

static int namrbd_json_get_u64(const char *json, const char *key, u64 *out)
{
	char pat[64];
	char *p;
	char *end;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	p = strchr(p, ':');
	if (!p)
		return -EBADMSG;
	p++;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;

	*out = simple_strtoull(p, &end, 10);
	if (end == p)
		return -EBADMSG;
	return 0;
}

static int namrbd_json_get_string(const char *json, const char *key, char *out, size_t out_len)
{
	char pat[64];
	char *p;
	char *end;
	size_t len;

	if (!out || out_len < 2)
		return -EINVAL;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	p = strchr(p, ':');
	if (!p)
		return -EBADMSG;
	p++;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	if (*p != '"')
		return -EBADMSG;
	p++;

	end = strchr(p, '"');
	if (!end)
		return -EBADMSG;

	len = end - p;
	if (len >= out_len)
		return -E2BIG;

	memcpy(out, p, len);
	out[len] = '\0';
	return 0;
}

static int namrbd_json_get_bool(const char *json, const char *key, bool *out)
{
	char pat[64];
	char *p;

	if (!out)
		return -EINVAL;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	p = strchr(p, ':');
	if (!p)
		return -EBADMSG;
	p++;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	if (!strncmp(p, "true", 4)) {
		*out = true;
		return 0;
	}
	if (!strncmp(p, "false", 5)) {
		*out = false;
		return 0;
	}
	return -EBADMSG;
}

static int namrbd_parse_volume_id_hex8(const char *s, u32 *out)
{
	int i;
	u32 v = 0;

	for (i = 0; i < 8; i++) {
		char c = s[i];
		u32 d;

		if (c >= '0' && c <= '9')
			d = c - '0';
		else if (c >= 'a' && c <= 'f')
			d = 10 + (c - 'a');
		else if (c >= 'A' && c <= 'F')
			d = 10 + (c - 'A');
		else
			return -EBADMSG;
		v = (v << 4) | d;
	}
	*out = v;
	return 0;
}

static void namrbd_format_volume_id_hex(char *buf, size_t len, u64 volume_id)
{
	scnprintf(buf, len, "%08x", (unsigned int)(volume_id & 0xffffffff));
}

static int namrbd_json_get_volume_id_hex(const char *json, u64 *out)
{
	char buf[16];
	u32 v32;
	int ret;

	ret = namrbd_json_get_string(json, "volume_id", buf, sizeof(buf));
	if (ret)
		return ret;
	if (strlen(buf) != 8)
		return -EBADMSG;
	if (namrbd_parse_volume_id_hex8(buf, &v32))
		return -EBADMSG;
	*out = v32;
	return 0;
}

static int namrbd_json_count_array_objects(const char *json, const char *key, u32 *count_out)
{
	char pat[64];
	char *p;
	char *arr;
	char *end;
	u32 count = 0;

	if (!count_out)
		return -EINVAL;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	arr = strchr(p, '[');
	if (!arr)
		return -EBADMSG;
	end = strchr(arr, ']');
	if (!end)
		return -EBADMSG;

	for (p = arr; p < end; p++) {
		if (*p == '{')
			count++;
	}
	*count_out = count;
	return 0;
}

static int namrbd_json_get_array_object(const char *json, const char *key, u32 index,
					char *out, size_t out_len)
{
	char pat[64];
	char *p;
	char *arr;
	char *end;
	u32 current_index = 0;

	if (!out || out_len < 3)
		return -EINVAL;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	arr = strchr(p, '[');
	if (!arr)
		return -EBADMSG;
	end = strchr(arr, ']');
	if (!end)
		return -EBADMSG;
	p = arr;
	while (p < end) {
		char *obj_start;
		char *obj_end;
		int depth = 0;
		size_t len;

		obj_start = strchr(p, '{');
		if (!obj_start || obj_start >= end)
			break;
		obj_end = obj_start;
		do {
			if (*obj_end == '{')
				depth++;
			else if (*obj_end == '}')
				depth--;
			obj_end++;
		} while (obj_end < end && depth > 0);
		if (depth != 0)
			return -EBADMSG;
		if (current_index == index) {
			len = obj_end - obj_start;
			if (len >= out_len)
				return -E2BIG;
			memcpy(out, obj_start, len);
			out[len] = '\0';
			return 0;
		}
		current_index++;
		p = obj_end;
	}
	return -ENOENT;
}

/* Copy the value of key when it is a JSON object { ... } into out. */
static int namrbd_json_get_object(const char *json, const char *key, char *out, size_t out_len)
{
	char pat[64];
	char *p;
	char *obj_start;
	char *obj_end;
	int depth;
	size_t len;

	if (!out || out_len < 3)
		return -EINVAL;

	scnprintf(pat, sizeof(pat), "\"%s\"", key);
	p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	p = strchr(p, ':');
	if (!p)
		return -EBADMSG;
	p++;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	if (*p != '{')
		return -EBADMSG;
	obj_start = p;
	depth = 1;
	p++;
	while (*p && depth > 0) {
		if (*p == '{')
			depth++;
		else if (*p == '}')
			depth--;
		p++;
	}
	obj_end = p - 1;
	if (depth != 0)
		return -EBADMSG;
	len = obj_end - obj_start + 1;
	if (len >= out_len)
		return -E2BIG;
	memcpy(out, obj_start, len);
	out[len] = '\0';
	return 0;
}

static int namrbd_parse_manifest_json(const char *json, u64 req_volume_id,
				      struct namrbd_attach_manifest *m)
{
	char *dataplane = NULL;
	char *auth_obj = NULL;
	u64 v;
	int ret;

	dataplane = kmalloc(512, GFP_KERNEL);
	if (!dataplane)
		return -ENOMEM;
	memset(m, 0, sizeof(*m));
	m->volume_id = req_volume_id;

	ret = namrbd_json_get_volume_id_hex(json, &v);
	if (ret)
		goto out_dp;
	if (v != req_volume_id) {
		ret = -EBADMSG;
		goto out_dp;
	}
	m->volume_id = v;

	ret = namrbd_json_get_u64(json, "generation", &v);
	if (ret)
		goto out_dp;
	m->generation = v;

	ret = namrbd_json_get_u64(json, "block_size", &v);
	if (ret)
		goto out_dp;
	if (!v || v > U32_MAX) {
		ret = -ERANGE;
		goto out_dp;
	}
	m->block_size = (u32)v;

	ret = namrbd_json_get_u64(json, "chunk_size_bytes", &v);
	if (!ret) {
		if (!v || v > U32_MAX) {
			ret = -ERANGE;
			goto out_dp;
		}
		m->chunk_size_bytes = (u32)v;
	} else {
		m->chunk_size_bytes = m->block_size;
	}

	ret = namrbd_json_get_u64(json, "size_bytes", &v);
	if (ret)
		goto out_dp;
	if (!v) {
		ret = -ERANGE;
		goto out_dp;
	}
	m->size_bytes = v;

	ret = namrbd_json_get_u64(json, "max_inflight_requests", &v);
	if (!ret) {
		if (!v || v > U32_MAX) {
			ret = -ERANGE;
			goto out_dp;
		}
		m->max_inflight_requests = (u32)v;
	} else {
		m->max_inflight_requests = 128;
	}

	ret = namrbd_json_get_u64(json, "max_inflight_bytes", &v);
	if (!ret) {
		if (!v) {
			ret = -ERANGE;
			goto out_dp;
		}
		m->max_inflight_bytes = v;
	} else {
		m->max_inflight_bytes = 8 * 1024 * 1024;
	}

	ret = namrbd_json_get_u64(json, "max_io_size", &v);
	if (!ret) {
		if (!v || v > U32_MAX) {
			ret = -ERANGE;
			goto out_dp;
		}
		m->max_io_size = (u32)v;
	} else {
		m->max_io_size = 128 * 1024;
	}

	ret = namrbd_json_get_u64(json, "max_zero_like_io_size", &v);
	if (!ret) {
		if (!v || v > U32_MAX) {
			ret = -ERANGE;
			goto out_dp;
		}
		m->max_zero_like_io_size = (u32)v;
	} else if (ret == -ENOENT) {
		m->max_zero_like_io_size = m->max_io_size;
	} else {
		goto out_dp;
	}

	{
		bool trusted = false;
		bool all_zero = false;
		int trusted_ret;
		int all_zero_ret;

		trusted_ret = namrbd_json_get_bool(json, "initial_zero_map_trusted", &trusted);
		all_zero_ret = namrbd_json_get_bool(json, "initial_zero_map_all_zero", &all_zero);
		if (trusted_ret && trusted_ret != -ENOENT) {
			ret = trusted_ret;
			goto out_dp;
		}
		if (all_zero_ret && all_zero_ret != -ENOENT) {
			ret = all_zero_ret;
			goto out_dp;
		}
		m->initial_zero_map_all_zero = !trusted_ret && !all_zero_ret && trusted && all_zero;
	}

	ret = namrbd_json_get_string(json, "attached_host_id", m->attached_host_id,
				     sizeof(m->attached_host_id));
	if (ret)
		goto out_dp;
	ret = namrbd_json_get_string(json, "attachment_id", m->attachment_id,
				     sizeof(m->attachment_id));
	if (ret)
		goto out_dp;
	ret = namrbd_json_get_u64(json, "attached_device_id", &v);
	if (ret)
		goto out_dp;
	if (v > U32_MAX) {
		ret = -ERANGE;
		goto out_dp;
	}
	m->attached_device_id = (u32)v;

	ret = namrbd_json_count_array_objects(json, "dataplane_endpoints", &m->dataplane_path_count);
	if (ret)
		goto out_dp;
	if (!m->dataplane_path_count) {
		ret = -ENOENT;
		goto out_dp;
	}
	if (m->dataplane_path_count > NAMRBD_TRANSPORT_MAX_PATHS)
		m->dataplane_path_count = NAMRBD_TRANSPORT_MAX_PATHS;
	{
		u32 i;
		u32 j;

		for (i = 0; i < m->dataplane_path_count; i++) {
			struct namrbd_transport_path *path = &m->dataplane_paths[i];
			bool use_tls;

			memset(path, 0, sizeof(*path));
			ret = namrbd_json_get_array_object(json, "dataplane_endpoints", i,
							 dataplane, 512);
			if (ret)
				goto out_dp;
			ret = namrbd_json_get_u64(dataplane, "path_id", &v);
			if (ret || v > U32_MAX) {
				ret = ret ? ret : -ERANGE;
				goto out_dp;
			}
			path->path_id = (u32)v;
			ret = namrbd_json_get_string(dataplane, "address", path->endpoint.address,
						     sizeof(path->endpoint.address));
			if (ret)
				goto out_dp;
			ret = namrbd_json_get_u64(dataplane, "port", &v);
			if (ret || !v || v > U16_MAX) {
				ret = ret ? ret : -ERANGE;
				goto out_dp;
			}
			path->endpoint.port = (u16)v;
			ret = namrbd_json_get_string(dataplane, "gateway_id", path->gateway_id,
						     sizeof(path->gateway_id));
			if (ret == -ENOENT)
				path->gateway_id[0] = '\0';
			else if (ret)
				goto out_dp;
			ret = namrbd_json_get_u64(dataplane, "priority", &v);
			if (ret == -ENOENT)
				path->priority = 0;
			else if (ret || v > U32_MAX) {
				ret = ret ? ret : -ERANGE;
				goto out_dp;
			} else {
				path->priority = (u32)v;
			}
			ret = namrbd_json_get_string(dataplane, "server_name",
						     path->endpoint.server_name,
						     sizeof(path->endpoint.server_name));
			if (ret == -ENOENT)
				path->endpoint.server_name[0] = '\0';
			else if (ret)
				goto out_dp;
			ret = namrbd_json_get_bool(dataplane, "use_tls", &use_tls);
			if (ret == -ENOENT)
				path->endpoint.use_tls = false;
			else if (ret)
				goto out_dp;
			else
				path->endpoint.use_tls = use_tls;
			for (j = 0; j < i; j++) {
				if (m->dataplane_paths[j].path_id == path->path_id) {
					ret = -EEXIST;
					goto out_dp;
				}
			}
		}
	}
	/* Phase C3: optional dataplane_auth (mode, token) for wire v2 */
	kfree(dataplane);
	dataplane = NULL;
	{
		auth_obj = kmalloc(4096, GFP_KERNEL);
		if (!auth_obj) {
			ret = -ENOMEM;
			goto out_dp;
		}
		ret = namrbd_json_get_object(json, "dataplane_auth", auth_obj, 4096);
		if (ret == 0) {
			ret = namrbd_json_get_string(auth_obj, "mode", m->dataplane_auth_mode,
						     sizeof(m->dataplane_auth_mode));
			if (ret == -ENOENT)
				m->dataplane_auth_mode[0] = '\0';
			else if (ret)
				goto out_auth;
			ret = namrbd_json_get_string(auth_obj, "token", m->dataplane_token,
						     sizeof(m->dataplane_token));
			if (ret == -ENOENT)
				m->dataplane_token[0] = '\0';
			else if (ret)
				goto out_auth;
			ret = namrbd_json_get_string(auth_obj, "session_key", m->dataplane_session_key,
						     sizeof(m->dataplane_session_key));
			if (ret == -ENOENT)
				m->dataplane_session_key[0] = '\0';
			else if (ret)
				goto out_auth;
		} else if (ret == -ENOENT) {
			m->dataplane_auth_mode[0] = '\0';
			m->dataplane_token[0] = '\0';
			m->dataplane_session_key[0] = '\0';
		} else {
			kfree(auth_obj);
			goto out_dp;
		}
		kfree(auth_obj);
	}
	kfree(dataplane);
	return 0;
out_auth:
	kfree(auth_obj);
out_dp:
	kfree(dataplane);
	return ret;
}

static int namrbd_validate_manifest(const struct namrbd_attach_manifest *m,
				    u32 req_device_id, u64 req_volume_id, const char *host_id)
{
	if (m->volume_id != req_volume_id)
		return -EBADMSG;
	if (!m->size_bytes)
		return -ERANGE;
	if (!m->block_size)
		return -ERANGE;
	if (!m->chunk_size_bytes || m->chunk_size_bytes < m->block_size ||
	    (m->chunk_size_bytes % m->block_size) != 0)
		return -ERANGE;
	if (!host_id || !host_id[0])
		return -EINVAL;
	if (strcmp(m->attached_host_id, host_id))
		return -EACCES;
	if (!m->attachment_id[0])
		return -EBADMSG;
	if (m->attached_device_id != req_device_id)
		return -EACCES;
	if (!m->dataplane_paths[0].endpoint.address[0])
		return -EBADMSG;
	if (!m->dataplane_paths[0].endpoint.port || !m->max_inflight_requests ||
	    !m->max_inflight_bytes || !m->max_io_size || !m->max_zero_like_io_size)
		return -ERANGE;
	if (!m->dataplane_path_count)
		return -ERANGE;
	return 0;
}

static int namrbd_http_status_to_errno(const char *resp)
{
	if (!strncmp(resp, "HTTP/1.1 200", 12) ||
	    !strncmp(resp, "HTTP/1.1 201", 12) ||
	    !strncmp(resp, "HTTP/1.1 204", 12))
		return 0;
	if (!strncmp(resp, "HTTP/1.1 400", 12))
		return -EINVAL;
	if (!strncmp(resp, "HTTP/1.1 401", 12) ||
	    !strncmp(resp, "HTTP/1.1 403", 12))
		return -EACCES;
	if (!strncmp(resp, "HTTP/1.1 404", 12))
		return -ENOENT;
	if (!strncmp(resp, "HTTP/1.1 409", 12))
		return -EEXIST;
	if (!strncmp(resp, "HTTP/1.1 5", 10))
		return -EREMOTEIO;
	return -EPROTO;
}

static int namrbd_http_simple_request(struct namrbd_rest_server *srv, const char *method,
				      const char *path, const char *body,
				      char *resp_out, size_t resp_out_len)
{
	struct namrbd_transport_endpoint ep = { 0 };
	struct socket *sock;
	struct msghdr msg = { 0 };
	struct kvec iov;
	char *req;
	char auth_hdr[320];
	char *resp;
	int len, ret;
	int body_len = body ? strlen(body) : 0;

	req = kmalloc(1024, GFP_KERNEL);
	if (!req)
		return -ENOMEM;
	resp = kmalloc(NAMRBD_MAX_HTTP_RESP, GFP_KERNEL);
	if (!resp) {
		kfree(req);
		return -ENOMEM;
	}

	if (srv->bearer_token[0])
		scnprintf(auth_hdr, sizeof(auth_hdr), "Authorization: Bearer %s\r\n",
			  srv->bearer_token);
	else
		auth_hdr[0] = '\0';

	len = scnprintf(req, 1024,
			"%s %s HTTP/1.1\r\n"
			"Host: %s:%u\r\n"
			"Connection: close\r\n"
			"Content-Type: application/json\r\n"
			"Content-Length: %d\r\n"
			"%s"
			"\r\n"
			"%s",
			method, path, srv->address, srv->port, body_len,
			auth_hdr,
			body ? body : "");
	if (len >= 1024) {
		ret = -E2BIG;
		goto out_free;
	}

	strscpy(ep.address, srv->address, sizeof(ep.address));
	ep.port = srv->port;
	ep.use_tls = srv->use_tls;
	ep.server_name[0] = '\0';
	ret = namrbd_transport_connect(&ep, &sock);
	if (ret < 0)
		goto out_release;

	iov.iov_base = req;
	iov.iov_len = len;
	ret = kernel_sendmsg(sock, &msg, &iov, 1, len);
	if (ret < 0)
		goto out_release;

	memset(resp, 0, NAMRBD_MAX_HTTP_RESP);
	iov.iov_base = resp;
	iov.iov_len = NAMRBD_MAX_HTTP_RESP - 1;
	ret = kernel_recvmsg(sock, &msg, &iov, 1, NAMRBD_MAX_HTTP_RESP - 1, 0);
	if (ret < 0)
		goto out_release;

	ret = namrbd_http_status_to_errno(resp);
	if (!ret && resp_out && resp_out_len)
		strscpy(resp_out, resp, resp_out_len);

out_release:
	sock_release(sock);
out_free:
	kfree(resp);
	kfree(req);
	return ret;
}

static int namrbd_rest_attach(u32 device_id, const char *host_id, u64 volume_id)
{
	struct namrbd_device_ctx *ctx;
	struct namrbd_rest_server *srv;
	char path[256];
	char body[256];
	char *resp;
	const char *resp_body;
	struct namrbd_attach_manifest *m;
	int ret;

	m = kmalloc(sizeof(*m), GFP_KERNEL);
	resp = kmalloc(NAMRBD_MAX_HTTP_RESP, GFP_KERNEL);
	if (!m || !resp) {
		kfree(resp);
		kfree(m);
		return -ENOMEM;
	}

	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_find_device_ctx_locked(device_id);
	if (!ctx) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		kfree(resp);
		kfree(m);
		return -ENODEV;
	}
	srv = namrbd_first_server_locked(ctx);
	if (!srv) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		kfree(resp);
		kfree(m);
		return -ENODEV;
	}

	{
		char volhex[16];

		namrbd_format_volume_id_hex(volhex, sizeof(volhex), volume_id);
		scnprintf(path, sizeof(path), "%s/volumes/%s/attach", srv->api_prefix, volhex);
	}
	scnprintf(body, sizeof(body), "{\"host_id\":\"%s\",\"device_id\":%u}",
		  host_id, device_id);

	ret = namrbd_http_simple_request(srv, "POST", path, body, resp, NAMRBD_MAX_HTTP_RESP);
	if (ret) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		kfree(resp);
		kfree(m);
		return ret;
	}

	resp_body = namrbd_find_http_body(resp);
	if (!resp_body) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		kfree(resp);
		kfree(m);
		return -EBADMSG;
	}

	ret = namrbd_parse_manifest_json(resp_body, volume_id, m);
	if (!ret)
		ret = namrbd_validate_manifest(m, device_id, volume_id, host_id);
	if (!ret)
		strscpy(ctx->attachment_id, m->attachment_id, sizeof(ctx->attachment_id));
	mutex_unlock(&namrbd_device_ctxs_lock);
	if (ret) {
		kfree(resp);
		kfree(m);
		return ret;
	}

	ret = namrbd_blk_configure_data_paths_device(device_id, m->dataplane_paths,
						     m->dataplane_path_count,
						     m->max_inflight_requests,
						     m->max_inflight_bytes,
						     m->max_io_size,
						     m->max_zero_like_io_size,
						     host_id,
						     m->dataplane_auth_mode,
						     m->dataplane_token,
						     m->dataplane_session_key);
	if (ret) {
		kfree(resp);
		kfree(m);
		return ret;
	}
	ret = namrbd_blk_activate_device_with_initial_zero_map(device_id, m->volume_id,
							      m->size_bytes,
							      m->block_size,
							      m->chunk_size_bytes,
							      m->generation,
							      m->initial_zero_map_all_zero);
	kfree(resp);
	kfree(m);
	return ret;
}

static int namrbd_rest_detach(u32 device_id, const char *host_id, u64 volume_id)
{
	struct namrbd_device_ctx *ctx;
	struct namrbd_rest_server *srv;
	char path[256];
	char body[256];
	int ret;

	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_find_device_ctx_locked(device_id);
	if (!ctx) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		return -ENODEV;
	}
	srv = namrbd_first_server_locked(ctx);
	if (!srv) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		return -ENODEV;
	}

	{
		char volhex[16];

		namrbd_format_volume_id_hex(volhex, sizeof(volhex), volume_id);
		scnprintf(path, sizeof(path), "%s/volumes/%s/detach", srv->api_prefix, volhex);
	}
	scnprintf(body, sizeof(body), "{\"host_id\":\"%s\",\"attachment_id\":\"%s\"}",
		  host_id, ctx->attachment_id);
	ret = namrbd_http_simple_request(srv, "POST", path, body, NULL, 0);
	mutex_unlock(&namrbd_device_ctxs_lock);
	if (!ret) {
		namrbd_blk_deactivate_device(device_id, volume_id);
		mutex_lock(&namrbd_device_ctxs_lock);
		ctx = namrbd_find_device_ctx_locked(device_id);
		if (ctx)
			ctx->attachment_id[0] = '\0';
		mutex_unlock(&namrbd_device_ctxs_lock);
	}
	return ret;
}

static int namrbd_encode_device_status(struct sk_buff *skb, int attr_type,
				       const struct namrbd_blk_status *st)
{
	struct nlattr *nest;
	struct nlattr *paths_nest;
	struct nlattr *lanes_nest;
	u32 i;

	nest = nla_nest_start(skb, attr_type);
	if (!nest)
		return -EMSGSIZE;
	if (nla_put_u32(skb, NAMRBD_STATUS_ATTR_DEVICE_ID, st->device_id) ||
	    nla_put_string(skb, NAMRBD_STATUS_ATTR_DISK_NAME, st->disk_name) ||
	    nla_put_u8(skb, NAMRBD_STATUS_ATTR_ATTACHED, st->attached) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_VOLUME_ID, st->volume_id,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_GENERATION, st->generation,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_PATH_COUNT, st->path_count) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_DOWN_MASK, st->down_mask,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_DEGRADED_MASK, st->degraded_mask,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_DRAINING_MASK, st->draining_mask,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_APPLIED_PATH_PLAN_REVISION, st->applied_path_plan_revision,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_ACTIVE_LANE_COUNT, st->active_lane_count) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_NR_HW_QUEUES, st->nr_hw_queues) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_TARGET_NR_HW_QUEUES, st->target_nr_hw_queues) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_QUEUE_TOPOLOGY_GENERATION,
			      st->queue_topology_generation, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_string(skb, NAMRBD_STATUS_ATTR_QUEUE_TOPOLOGY_STATE,
			   st->queue_topology_state) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_LANE_REMAP_COUNT, st->lane_remap_count,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_LAST_LANE_REMAPPED_LANES,
			st->last_lane_remapped_lanes) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_LAST_LANE_REMAP_JIFFIES,
			      st->last_lane_remap_jiffies, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_string(skb, NAMRBD_STATUS_ATTR_LAST_LANE_REMAP_REASON,
			   st->last_lane_remap_reason) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_NO_PATH_RETRY_MODE,
			st->no_path_retry_mode) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_NO_PATH_RETRY_SECONDS,
			st->no_path_retry_seconds) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_NO_PATH_STATE,
			st->no_path_state) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_SINCE_JIFFIES,
			      st->no_path_since_jiffies, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_RETRY_DEADLINE_JIFFIES,
			      st->no_path_retry_deadline_jiffies,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_WAKEUP_JIFFIES,
			      st->last_no_path_wakeup_jiffies,
			      NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_QUEUED_REQS,
			      st->no_path_queued_reqs, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_REQUEUED_REQS,
			      st->no_path_requeued_reqs, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_FAILED_REQS,
			      st->no_path_failed_reqs, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_RECOVERED_REQS,
			      st->no_path_recovered_reqs, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_NO_PATH_ENTER_COUNT,
			      st->no_path_enter_count, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_REASON,
			st->last_no_path_reason) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_OP,
			st->last_no_path_op) ||
	    nla_put_u32(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_ELIGIBLE_PATHS,
			st->last_no_path_eligible_paths) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_TRIED_MASK,
			      st->last_no_path_tried_mask, NAMRBD_STATUS_ATTR_UNSPEC) ||
	    nla_put_u64_64bit(skb, NAMRBD_STATUS_ATTR_LAST_NO_PATH_JIFFIES,
			      st->last_no_path_jiffies, NAMRBD_STATUS_ATTR_UNSPEC)) {
		nla_nest_cancel(skb, nest);
		return -EMSGSIZE;
	}
	paths_nest = nla_nest_start(skb, NAMRBD_STATUS_ATTR_PATHS);
	if (!paths_nest) {
		nla_nest_cancel(skb, nest);
		return -EMSGSIZE;
	}
	for (i = 0; i < st->path_count; i++) {
		struct nlattr *entry = nla_nest_start(skb, NAMRBD_STATUS_PATH_ATTR_ENTRY);
		if (!entry ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_PATH_ID, st->paths[i].path_id) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_STATE, st->paths[i].state) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_CONSECUTIVE_ERRORS, st->paths[i].consecutive_errors) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_LAST_ERRNO, st->paths[i].last_errno) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_LAST_WIRE_STATUS, st->paths[i].last_wire_status) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_PRIORITY, st->paths[i].priority) ||
		    nla_put_u8(skb, NAMRBD_STATUS_PATH_ATTR_CONNECTED, st->paths[i].connected) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_INFLIGHT, st->paths[i].inflight) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_PENDING, st->paths[i].pending) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_PENDING_HIGH_WATER, st->paths[i].pending_high_water) ||
		    nla_put_u32(skb, NAMRBD_STATUS_PATH_ATTR_OUTSTANDING_LIMIT, st->paths[i].outstanding_limit) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_PATH_ATTR_SUBMITTED, st->paths[i].submitted,
				      NAMRBD_STATUS_PATH_ATTR_UNSPEC) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_PATH_ATTR_COMPLETED, st->paths[i].completed,
				      NAMRBD_STATUS_PATH_ATTR_UNSPEC) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_PATH_ATTR_RETRIES, st->paths[i].retries,
				      NAMRBD_STATUS_PATH_ATTR_UNSPEC) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_PATH_ATTR_CONN_OPENS, st->paths[i].conn_opens,
				      NAMRBD_STATUS_PATH_ATTR_UNSPEC) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_PATH_ATTR_CONN_RESETS, st->paths[i].conn_resets,
				      NAMRBD_STATUS_PATH_ATTR_UNSPEC) ||
		    nla_put_u16(skb, NAMRBD_STATUS_PATH_ATTR_PORT, st->paths[i].port) ||
		    nla_put_u8(skb, NAMRBD_STATUS_PATH_ATTR_USE_TLS, st->paths[i].use_tls) ||
		    (st->paths[i].gateway_id[0] &&
		     nla_put_string(skb, NAMRBD_STATUS_PATH_ATTR_GATEWAY_ID, st->paths[i].gateway_id)) ||
		    (st->paths[i].address[0] &&
		     nla_put_string(skb, NAMRBD_STATUS_PATH_ATTR_ADDRESS, st->paths[i].address)) ||
		    (st->paths[i].server_name[0] &&
		     nla_put_string(skb, NAMRBD_STATUS_PATH_ATTR_SERVER_NAME, st->paths[i].server_name))) {
			if (entry)
				nla_nest_cancel(skb, entry);
			nla_nest_cancel(skb, paths_nest);
			nla_nest_cancel(skb, nest);
			return -EMSGSIZE;
		}
		nla_nest_end(skb, entry);
	}
	nla_nest_end(skb, paths_nest);
	lanes_nest = nla_nest_start(skb, NAMRBD_STATUS_ATTR_LANES);
	if (!lanes_nest) {
		nla_nest_cancel(skb, nest);
		return -EMSGSIZE;
	}
	for (i = 0; i < st->active_lane_count; i++) {
		struct nlattr *entry = nla_nest_start(skb, NAMRBD_STATUS_LANE_ATTR_ENTRY);
		if (!entry ||
		    nla_put_u32(skb, NAMRBD_STATUS_LANE_ATTR_LANE_ID, st->lanes[i].lane_id) ||
		    nla_put_u32(skb, NAMRBD_STATUS_LANE_ATTR_PREFERRED_PATH_ID,
				st->lanes[i].preferred_path_id) ||
		    nla_put_u32(skb, NAMRBD_STATUS_LANE_ATTR_FALLBACK_PATH_ID,
				st->lanes[i].fallback_path_id) ||
		    nla_put_u32(skb, NAMRBD_STATUS_LANE_ATTR_READINESS,
				st->lanes[i].readiness) ||
		    nla_put_u64_64bit(skb, NAMRBD_STATUS_LANE_ATTR_DISPATCH_REQS,
				      st->lanes[i].dispatch_reqs,
				      NAMRBD_STATUS_LANE_ATTR_UNSPEC)) {
			if (entry)
				nla_nest_cancel(skb, entry);
			nla_nest_cancel(skb, lanes_nest);
			nla_nest_cancel(skb, nest);
			return -EMSGSIZE;
		}
		nla_nest_end(skb, entry);
	}
	nla_nest_end(skb, lanes_nest);
	nla_nest_end(skb, nest);
	return 0;
}

static int namrbd_reply_create_device(struct genl_info *info, u32 device_id, const char *disk_name)
{
	struct sk_buff *skb;
	void *hdr;

	skb = genlmsg_new(NLMSG_GOODSIZE, GFP_KERNEL);
	if (!skb)
		return -ENOMEM;

	hdr = genlmsg_put(skb, info->snd_portid, info->snd_seq, &namrbd_genl_family, 0,
			  NAMRBD_CMD_CREATE_DEVICE);
	if (!hdr) {
		nlmsg_free(skb);
		return -EMSGSIZE;
	}
	if (nla_put_u32(skb, NAMRBD_ATTR_DEVICE_ID, device_id) ||
	    nla_put_string(skb, NAMRBD_ATTR_DISK_NAME, disk_name)) {
		genlmsg_cancel(skb, hdr);
		nlmsg_free(skb);
		return -EMSGSIZE;
	}
	genlmsg_end(skb, hdr);
	return genlmsg_reply(skb, info);
}

static int namrbd_reply_status(struct genl_info *info, const struct namrbd_blk_status *st)
{
	struct sk_buff *skb;
	void *hdr;
	int ret;

	skb = genlmsg_new(NLMSG_GOODSIZE, GFP_KERNEL);
	if (!skb)
		return -ENOMEM;

	hdr = genlmsg_put(skb, info->snd_portid, info->snd_seq, &namrbd_genl_family, 0,
			  NAMRBD_CMD_GET_STATUS);
	if (!hdr) {
		nlmsg_free(skb);
		return -EMSGSIZE;
	}
	ret = namrbd_encode_device_status(skb, NAMRBD_ATTR_DEVICE_STATUS, st);
	if (ret) {
		genlmsg_cancel(skb, hdr);
		nlmsg_free(skb);
		return ret;
	}
	genlmsg_end(skb, hdr);
	return genlmsg_reply(skb, info);
}

static int namrbd_reply_list_devices(struct genl_info *info, struct namrbd_blk_status *sts,
				     u32 count)
{
	struct sk_buff *skb;
	struct nlattr *list_nest;
	void *hdr;
	u32 i;

	skb = genlmsg_new(NLMSG_GOODSIZE, GFP_KERNEL);
	if (!skb)
		return -ENOMEM;

	hdr = genlmsg_put(skb, info->snd_portid, info->snd_seq, &namrbd_genl_family, 0,
			  NAMRBD_CMD_LIST_DEVICES);
	if (!hdr) {
		nlmsg_free(skb);
		return -EMSGSIZE;
	}

	list_nest = nla_nest_start(skb, NAMRBD_ATTR_DEVICE_LIST);
	if (!list_nest) {
		genlmsg_cancel(skb, hdr);
		nlmsg_free(skb);
		return -EMSGSIZE;
	}
	for (i = 0; i < count; i++) {
		if (namrbd_encode_device_status(skb, NAMRBD_ATTR_DEVICE_STATUS, &sts[i])) {
			nla_nest_cancel(skb, list_nest);
			genlmsg_cancel(skb, hdr);
			nlmsg_free(skb);
			return -EMSGSIZE;
		}
	}
	nla_nest_end(skb, list_nest);
	genlmsg_end(skb, hdr);
	return genlmsg_reply(skb, info);
}

static int namrbd_cmd_create_device(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_blk_status *st;
	u32 device_id;
	int ret;

	st = kzalloc(sizeof(*st), GFP_KERNEL);
	if (!st)
		return -ENOMEM;
	ret = namrbd_blk_create(&device_id);
	if (ret) {
		kfree(st);
		return ret;
	}
	ret = namrbd_blk_get_status(device_id, st);
	if (ret) {
		namrbd_blk_destroy(device_id);
		kfree(st);
		return ret;
	}
	ret = namrbd_reply_create_device(info, st->device_id, st->disk_name);
	kfree(st);
	return ret;
}

static int namrbd_cmd_destroy_device(struct sk_buff *skb, struct genl_info *info)
{
	u32 device_id;
	int ret;

	if (!info->attrs[NAMRBD_ATTR_DEVICE_ID])
		return -EINVAL;
	device_id = nla_get_u32(info->attrs[NAMRBD_ATTR_DEVICE_ID]);

	ret = namrbd_blk_destroy(device_id);
	if (ret)
		return ret;

	mutex_lock(&namrbd_device_ctxs_lock);
	namrbd_remove_device_ctx_locked(device_id);
	mutex_unlock(&namrbd_device_ctxs_lock);
	return 0;
}

static int namrbd_cmd_config_rest(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_blk_status *st;
	struct namrbd_device_ctx *ctx;
	struct nlattr *na;
	int rem;
	u32 device_id;
	int ret;

	if (!info->attrs[NAMRBD_ATTR_DEVICE_ID] || !info->attrs[NAMRBD_ATTR_SERVERS])
		return -EINVAL;
	device_id = nla_get_u32(info->attrs[NAMRBD_ATTR_DEVICE_ID]);
	st = kzalloc(sizeof(*st), GFP_KERNEL);
	if (!st)
		return -ENOMEM;
	ret = namrbd_blk_get_status(device_id, st);
	kfree(st);
	if (ret)
		return ret;

	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_get_or_create_device_ctx_locked(device_id);
	if (!ctx) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		return -ENOMEM;
	}
	namrbd_free_servers(&ctx->servers);

	nla_for_each_nested(na, info->attrs[NAMRBD_ATTR_SERVERS], rem) {
		struct namrbd_rest_server *s;
		struct nlattr *tb[NAMRBD_SERVER_ATTR_MAX + 1];

		if ((nla_type(na) & NLA_TYPE_MASK) != NAMRBD_ATTR_SERVER_ENTRY)
			continue;

		ret = nla_parse_nested(tb, NAMRBD_SERVER_ATTR_MAX, na, namrbd_server_policy, NULL);
		if (ret < 0)
			goto out_err;

		if (!tb[NAMRBD_SERVER_ATTR_ADDRESS] || !tb[NAMRBD_SERVER_ATTR_PORT] ||
		    !tb[NAMRBD_SERVER_ATTR_API_PREFIX]) {
			ret = -EINVAL;
			goto out_err;
		}

		s = kzalloc(sizeof(*s), GFP_KERNEL);
		if (!s) {
			ret = -ENOMEM;
			goto out_err;
		}

		s->id = tb[NAMRBD_SERVER_ATTR_ID] ? nla_get_u32(tb[NAMRBD_SERVER_ATTR_ID]) : 0;
		strscpy(s->address, nla_data(tb[NAMRBD_SERVER_ATTR_ADDRESS]), sizeof(s->address));
		s->port = nla_get_u16(tb[NAMRBD_SERVER_ATTR_PORT]);
		s->use_tls = tb[NAMRBD_SERVER_ATTR_USE_TLS] ?
				 nla_get_u8(tb[NAMRBD_SERVER_ATTR_USE_TLS]) != 0 :
				 false;
		strscpy(s->api_prefix, nla_data(tb[NAMRBD_SERVER_ATTR_API_PREFIX]),
			sizeof(s->api_prefix));
		if (tb[NAMRBD_SERVER_ATTR_BEARER_TOKEN])
			strscpy(s->bearer_token, nla_data(tb[NAMRBD_SERVER_ATTR_BEARER_TOKEN]),
				sizeof(s->bearer_token));
		list_add_tail(&s->list, &ctx->servers);
	}
	mutex_unlock(&namrbd_device_ctxs_lock);
	return 0;

out_err:
	namrbd_free_servers(&ctx->servers);
	mutex_unlock(&namrbd_device_ctxs_lock);
	return ret;
}

static int namrbd_parse_device_req(struct nlattr *attr, u32 *device_id, const char **host_id,
				   u64 *volume_id)
{
	struct nlattr *tb[NAMRBD_REQ_ATTR_MAX + 1];
	int ret;

	if (!attr || !device_id || !host_id || !volume_id)
		return -EINVAL;

	ret = nla_parse_nested(tb, NAMRBD_REQ_ATTR_MAX, attr, namrbd_req_policy, NULL);
	if (ret < 0)
		return ret;
	if (!tb[NAMRBD_REQ_ATTR_DEVICE_ID] || !tb[NAMRBD_REQ_ATTR_HOST_ID] ||
	    !tb[NAMRBD_REQ_ATTR_VOLUME_ID])
		return -EINVAL;

	*device_id = nla_get_u32(tb[NAMRBD_REQ_ATTR_DEVICE_ID]);
	*host_id = nla_data(tb[NAMRBD_REQ_ATTR_HOST_ID]);
	*volume_id = nla_get_u64(tb[NAMRBD_REQ_ATTR_VOLUME_ID]);
	return 0;
}

static int namrbd_parse_device_req_optional_host(struct nlattr *attr, u32 *device_id,
						 const char **host_id, u64 *volume_id,
						 const char **manifest_json)
{
	struct nlattr *tb[NAMRBD_ATTR_MAX + 1];
	int ret;

	if (!attr || !device_id || !volume_id)
		return -EINVAL;

	ret = nla_parse_nested(tb, NAMRBD_ATTR_MAX, attr, namrbd_attach_manifest_req_policy, NULL);
	if (ret < 0)
		return ret;
	if (!tb[NAMRBD_REQ_ATTR_DEVICE_ID] || !tb[NAMRBD_REQ_ATTR_VOLUME_ID])
		return -EINVAL;

	*device_id = nla_get_u32(tb[NAMRBD_REQ_ATTR_DEVICE_ID]);
	*volume_id = nla_get_u64(tb[NAMRBD_REQ_ATTR_VOLUME_ID]);
	if (host_id)
		*host_id = tb[NAMRBD_REQ_ATTR_HOST_ID] ? nla_data(tb[NAMRBD_REQ_ATTR_HOST_ID]) : NULL;
	if (manifest_json)
		*manifest_json = tb[NAMRBD_ATTR_MANIFEST_JSON] ? nla_data(tb[NAMRBD_ATTR_MANIFEST_JSON]) : NULL;
	return 0;
}

static int namrbd_cmd_attach(struct sk_buff *skb, struct genl_info *info)
{
	u32 device_id;
	const char *host_id;
	u64 volume_id;
	int ret;

	ret = namrbd_parse_device_req(info->attrs[NAMRBD_ATTR_ATTACH_REQ], &device_id, &host_id,
				      &volume_id);
	if (ret)
		return ret;
	return namrbd_rest_attach(device_id, host_id, volume_id);
}

static int namrbd_cmd_detach(struct sk_buff *skb, struct genl_info *info)
{
	u32 device_id;
	const char *host_id;
	u64 volume_id;
	int ret;

	ret = namrbd_parse_device_req(info->attrs[NAMRBD_ATTR_DETACH_REQ], &device_id, &host_id,
				      &volume_id);
	if (ret)
		return ret;
	return namrbd_rest_detach(device_id, host_id, volume_id);
}

static int namrbd_cmd_attach_manifest(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_device_ctx *ctx;
	struct namrbd_attach_manifest *manifest;
	const char *host_id;
	const char *manifest_json;
	u32 device_id;
	u64 volume_id;
	int ret;

	ret = namrbd_parse_device_req_optional_host(info->attrs[NAMRBD_ATTR_ATTACH_REQ], &device_id,
						    &host_id, &volume_id, &manifest_json);
	if (ret)
		return ret;
	if (!host_id || !manifest_json)
		return -EINVAL;

	manifest = kmalloc(sizeof(*manifest), GFP_KERNEL);
	if (!manifest)
		return -ENOMEM;
	ret = namrbd_parse_manifest_json(manifest_json, volume_id, manifest);
	if (ret)
		goto out;
	ret = namrbd_validate_manifest(manifest, device_id, volume_id, host_id);
	if (ret)
		goto out;

	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_get_or_create_device_ctx_locked(device_id);
	if (!ctx) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		ret = -ENOMEM;
		goto out;
	}
	strscpy(ctx->attachment_id, manifest->attachment_id, sizeof(ctx->attachment_id));
	mutex_unlock(&namrbd_device_ctxs_lock);

	ret = namrbd_blk_configure_data_paths_device(device_id, manifest->dataplane_paths,
						     manifest->dataplane_path_count,
						     manifest->max_inflight_requests,
						     manifest->max_inflight_bytes,
						     manifest->max_io_size,
						     manifest->max_zero_like_io_size,
						     host_id,
						     manifest->dataplane_auth_mode,
						     manifest->dataplane_token,
						     manifest->dataplane_session_key);
	if (ret)
		goto out;
	ret = namrbd_blk_activate_device_with_initial_zero_map(device_id, manifest->volume_id,
							      manifest->size_bytes,
							      manifest->block_size,
							      manifest->chunk_size_bytes,
							      manifest->generation,
							      manifest->initial_zero_map_all_zero);
out:
	kfree(manifest);
	return ret;
}

static int namrbd_cmd_reconfigure_data_paths(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_device_ctx *ctx;
	struct namrbd_attach_manifest *manifest;
	const char *host_id;
	const char *manifest_json;
	u32 device_id;
	u64 volume_id;
	int ret;

	ret = namrbd_parse_device_req_optional_host(info->attrs[NAMRBD_ATTR_ATTACH_REQ], &device_id,
						    &host_id, &volume_id, &manifest_json);
	if (ret)
		return ret;
	if (!host_id || !manifest_json)
		return -EINVAL;

	manifest = kmalloc(sizeof(*manifest), GFP_KERNEL);
	if (!manifest)
		return -ENOMEM;
	ret = namrbd_parse_manifest_json(manifest_json, volume_id, manifest);
	if (ret)
		goto out;
	ret = namrbd_validate_manifest(manifest, device_id, volume_id, host_id);
	if (ret)
		goto out;

	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_get_or_create_device_ctx_locked(device_id);
	if (!ctx) {
		mutex_unlock(&namrbd_device_ctxs_lock);
		ret = -ENOMEM;
		goto out;
	}
	strscpy(ctx->attachment_id, manifest->attachment_id, sizeof(ctx->attachment_id));
	mutex_unlock(&namrbd_device_ctxs_lock);

	ret = namrbd_blk_configure_data_paths_device(device_id, manifest->dataplane_paths,
						     manifest->dataplane_path_count,
						     manifest->max_inflight_requests,
						     manifest->max_inflight_bytes,
						     manifest->max_io_size,
						     manifest->max_zero_like_io_size,
						     host_id,
						     manifest->dataplane_auth_mode,
						     manifest->dataplane_token,
						     manifest->dataplane_session_key);
out:
	kfree(manifest);
	return ret;
}

static int namrbd_cmd_detach_local(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_device_ctx *ctx;
	u32 device_id;
	u64 volume_id;
	int ret;

	ret = namrbd_parse_device_req_optional_host(info->attrs[NAMRBD_ATTR_DETACH_REQ], &device_id,
						    NULL, &volume_id, NULL);
	if (ret)
		return ret;
	namrbd_blk_deactivate_device(device_id, volume_id);
	mutex_lock(&namrbd_device_ctxs_lock);
	ctx = namrbd_find_device_ctx_locked(device_id);
	if (ctx)
		ctx->attachment_id[0] = '\0';
	mutex_unlock(&namrbd_device_ctxs_lock);
	return 0;
}

static int namrbd_cmd_update_path_plan(struct sk_buff *skb, struct genl_info *info)
{
	u32 device_id;
	u64 down_mask_bits = 0;
	u64 degraded_mask_bits = 0;
	u64 draining_mask_bits = 0;
	u64 path_plan_revision = 0;

	if (!info->attrs[NAMRBD_ATTR_DEVICE_ID])
		return -EINVAL;
	device_id = nla_get_u32(info->attrs[NAMRBD_ATTR_DEVICE_ID]);
	if (info->attrs[NAMRBD_ATTR_DOWN_MASK])
		down_mask_bits = nla_get_u64(info->attrs[NAMRBD_ATTR_DOWN_MASK]);
	if (info->attrs[NAMRBD_ATTR_DEGRADED_MASK])
		degraded_mask_bits = nla_get_u64(info->attrs[NAMRBD_ATTR_DEGRADED_MASK]);
	if (info->attrs[NAMRBD_ATTR_DRAINING_MASK])
		draining_mask_bits = nla_get_u64(info->attrs[NAMRBD_ATTR_DRAINING_MASK]);
	if (info->attrs[NAMRBD_ATTR_PATH_PLAN_REVISION])
		path_plan_revision = nla_get_u64(info->attrs[NAMRBD_ATTR_PATH_PLAN_REVISION]);

	return namrbd_blk_update_path_masks_device(device_id, path_plan_revision, down_mask_bits,
						   degraded_mask_bits, draining_mask_bits);
}

static int namrbd_cmd_resize_device(struct sk_buff *skb, struct genl_info *info)
{
	u32 device_id;
	u64 volume_id;
	u64 generation;
	u64 size_bytes;

	if (!info->attrs[NAMRBD_ATTR_DEVICE_ID] ||
	    !info->attrs[NAMRBD_ATTR_VOLUME_ID] ||
	    !info->attrs[NAMRBD_ATTR_GENERATION] ||
	    !info->attrs[NAMRBD_ATTR_SIZE_BYTES])
		return -EINVAL;
	device_id = nla_get_u32(info->attrs[NAMRBD_ATTR_DEVICE_ID]);
	volume_id = nla_get_u64(info->attrs[NAMRBD_ATTR_VOLUME_ID]);
	generation = nla_get_u64(info->attrs[NAMRBD_ATTR_GENERATION]);
	size_bytes = nla_get_u64(info->attrs[NAMRBD_ATTR_SIZE_BYTES]);
	return namrbd_blk_resize_device(device_id, volume_id, generation, size_bytes);
}

static int namrbd_cmd_get_status(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_blk_status *st;
	u32 device_id;
	int ret;

	if (!info->attrs[NAMRBD_ATTR_DEVICE_ID])
		return -EINVAL;
	device_id = nla_get_u32(info->attrs[NAMRBD_ATTR_DEVICE_ID]);
	st = kzalloc(sizeof(*st), GFP_KERNEL);
	if (!st)
		return -ENOMEM;
	ret = namrbd_blk_get_status(device_id, st);
	if (ret) {
		kfree(st);
		return ret;
	}
	ret = namrbd_reply_status(info, st);
	kfree(st);
	return ret;
}

static int namrbd_cmd_list_devices(struct sk_buff *skb, struct genl_info *info)
{
	struct namrbd_blk_status *sts;
	u32 count = 0;
	int ret;

	sts = kvcalloc(NAMRBD_MAX_LIST_DEVICES, sizeof(*sts), GFP_KERNEL);
	if (!sts)
		return -ENOMEM;
	ret = namrbd_blk_list_devices(sts, NAMRBD_MAX_LIST_DEVICES, &count);
	if (ret) {
		kvfree(sts);
		return ret;
	}
	ret = namrbd_reply_list_devices(info, sts, count);
	kvfree(sts);
	return ret;
}

static const struct genl_ops namrbd_ops[] = {
	{
		.cmd = NAMRBD_CMD_CREATE_DEVICE,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_create_device,
	},
	{
		.cmd = NAMRBD_CMD_DESTROY_DEVICE,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_destroy_device,
	},
	{
		.cmd = NAMRBD_CMD_CONFIG_REST,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_config_rest,
	},
	{
		.cmd = NAMRBD_CMD_ATTACH,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_attach,
	},
	{
		.cmd = NAMRBD_CMD_DETACH,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_detach,
	},
	{
		.cmd = NAMRBD_CMD_GET_STATUS,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_get_status,
	},
	{
		.cmd = NAMRBD_CMD_LIST_DEVICES,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_list_devices,
	},
	{
		.cmd = NAMRBD_CMD_ATTACH_MANIFEST,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_attach_manifest,
	},
	{
		.cmd = NAMRBD_CMD_DETACH_LOCAL,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_detach_local,
	},
	{
		.cmd = NAMRBD_CMD_UPDATE_PATH_PLAN,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_update_path_plan,
	},
	{
		.cmd = NAMRBD_CMD_RECONFIGURE_DATA_PATHS,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_reconfigure_data_paths,
	},
	{
		.cmd = NAMRBD_CMD_RESIZE_DEVICE,
		.flags = 0,
		.policy = namrbd_policy,
		.doit = namrbd_cmd_resize_device,
	},
};

static struct genl_family namrbd_genl_family __ro_after_init = {
	.name = NAMRBD_GENL_FAMILY_NAME,
	.version = NAMRBD_GENL_VERSION,
	.maxattr = NAMRBD_ATTR_MAX,
	.module = THIS_MODULE,
	.ops = namrbd_ops,
	.n_ops = ARRAY_SIZE(namrbd_ops),
};

static int __init namrbd_ctrl_init(void)
{
	return genl_register_family(&namrbd_genl_family);
}

static void __exit namrbd_ctrl_exit(void)
{
	struct namrbd_device_ctx *ctx, *tmp;

	mutex_lock(&namrbd_device_ctxs_lock);
	list_for_each_entry_safe(ctx, tmp, &namrbd_device_ctxs, list) {
		list_del(&ctx->list);
		namrbd_free_servers(&ctx->servers);
		kfree(ctx);
	}
	mutex_unlock(&namrbd_device_ctxs_lock);
	genl_unregister_family(&namrbd_genl_family);
}

module_init(namrbd_ctrl_init);
module_exit(namrbd_ctrl_exit);

MODULE_DESCRIPTION("NAMRBD control path module");
MODULE_AUTHOR("Taewoong Kim (taewoong.kim@gmail.com)");
MODULE_LICENSE("GPL");
