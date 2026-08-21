// SPDX-License-Identifier: GPL-2.0-only
/*
 * NAMRBD host block-device data plane.
 *
 * This module owns the host-local block-device runtime for NAMRBD:
 * - gendisk and blk-mq queue registration
 * - Linux block request dispatch and completion
 * - device capacity, activation, resize, and local detach state
 * - manifest-provided gateway dataplane paths, lane mapping, and path health
 * - no-path retry/queue/fail policy, counters, sysfs/debugfs status
 * - persistent gateway TCP dataplane connections, request IDs, pending request
 *   tracking, response validation, and wire v1/v2 framing
 *
 * The module applies control decisions supplied through namrbd_ctrl.ko. It does
 * not decide attachment ownership, mint generations, call SBS metadata APIs, or
 * own replica/EC placement, repair, drain, or rebalance policy. Those decisions
 * remain in gateway/control metadata and SBS services.
 *
 * A memory-backed mode remains available for local bring-up and compatibility
 * paths, but an attached production device is expected to use manifest-provided
 * gateway dataplane endpoints.
 */

#include <linux/blk-mq.h>
#include <linux/blkdev.h>
#include <linux/bitmap.h>
#include <linux/version.h>
#include <linux/completion.h>
#include <linux/crc32c.h>
#include <linux/cpumask.h>
#include <linux/debugfs.h>
#include <linux/err.h>
#include <linux/errno.h>
#include <linux/highmem.h>
#include <linux/idr.h>
#include <linux/init.h>
#include <linux/jiffies.h>
#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/list.h>
#include <linux/math64.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/seq_file.h>
#include <linux/semaphore.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/string.h>
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 0, 0)
#include <linux/unaligned.h>
#else
#include <asm-generic/unaligned.h>
#endif
#include <linux/vmalloc.h>

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 0, 0)
typedef enum req_op namrbd_req_op_t;
#else
typedef unsigned int namrbd_req_op_t;
#endif

#include <net/sock.h>
#include <crypto/hash.h>

#include "namrbd_transport.h"

#define NAMRBD_DISK_NAME_PREFIX "namrbd"
#define NAMRBD_QUEUE_DEPTH 128
#define NAMRBD_SECTOR_SIZE 512
#define NAMRBD_SECTOR_SHIFT 9
#define NAMRBD_BLOCK_SIZE 4096
#define NAMRBD_ZERO_MAP_GRANULE_BYTES (64U * 1024U)
#define NAMRBD_MAX_PATHS 16
#define NAMRBD_WIRE_HDR_LEN 56
#define NAMRBD_WIRE_RESP_LEN 76
#define NAMRBD_WIRE_MAGIC 0x4E4D4252
#define NAMRBD_WIRE_VERSION 1
#define NAMRBD_WIRE_OP_READ 0x0001
#define NAMRBD_WIRE_OP_WRITE 0x0002
#define NAMRBD_WIRE_OP_FLUSH 0x0003
#define NAMRBD_WIRE_OP_DISCARD 0x0004
#define NAMRBD_WIRE_OP_WRITE_ZEROES 0x0005
#define NAMRBD_WIRE_OP_PATH_PROBE 0x0007
#define NAMRBD_WIRE_OP_READ_RESP 0x8001
#define NAMRBD_WIRE_OP_WRITE_RESP 0x8002
#define NAMRBD_WIRE_OP_FLUSH_RESP 0x8003
#define NAMRBD_WIRE_OP_DISCARD_RESP 0x8004
#define NAMRBD_WIRE_OP_WRITE_ZEROES_RESP 0x8005
#define NAMRBD_WIRE_OP_ERROR_RESP 0x8fff
/* Wire v2 (Phase C3) */
#define NAMRBD_WIREV2_HDR_LEN 76
#define NAMRBD_WIREV2_RESP_LEN 96
#define NAMRBD_WIREV2_AUTH_TAG_LEN 32
#define NAMRBD_WIREV2_OP_HELLO    0x0010
#define NAMRBD_WIREV2_OP_HELLO_ACK 0x0011
#define NAMRBD_WIREV2_OP_READ     0x0001
#define NAMRBD_WIREV2_OP_WRITE    0x0002
#define NAMRBD_WIREV2_OP_DISCARD  0x0004
#define NAMRBD_WIREV2_OP_WRITE_ZEROES 0x0005
#define NAMRBD_WIREV2_MAGIC       0x4E4D4252
#define NAMRBD_WIREV2_VERSION     2
#define NAMRBD_DEFAULT_DATA_PORT 9700
#define NAMRBD_DEFAULT_MAX_IO_SIZE (128 * 1024)
#define NAMRBD_DEFAULT_MAX_DATA_IO_SIZE NAMRBD_DEFAULT_MAX_IO_SIZE
#define NAMRBD_DEFAULT_MAX_INFLIGHT_REQS 128
#define NAMRBD_DEFAULT_MAX_INFLIGHT_BYTES (8 * 1024 * 1024)
#define NAMRBD_RESOURCE_REQUEUE_BACKSTOP_MS 10
#define NAMRBD_PATH_DEGRADE_THRESHOLD 1
#define NAMRBD_PATH_DOWN_THRESHOLD 3
#define NAMRBD_DEFAULT_MAX_IO_SECTORS (NAMRBD_DEFAULT_MAX_IO_SIZE / NAMRBD_SECTOR_SIZE)
#define NAMRBD_MAX_HOST_ID_LEN 128

static void namrbd_set_disk_capacity(struct gendisk *disk, sector_t sectors, bool notify)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 10, 0)
	if (notify) {
		set_capacity_and_notify(disk, sectors);
		return;
	}
#else
	(void)notify;
#endif
	set_capacity(disk, sectors);
}

static unsigned long long size_mb = 64;
module_param(size_mb, ullong, 0644);
MODULE_PARM_DESC(size_mb, "RAM backing size in MiB");

static int nr_paths = 2;
module_param(nr_paths, int, 0644);
MODULE_PARM_DESC(nr_paths, "number of simulated paths");

static uint default_active_lanes = 2;
module_param(default_active_lanes, uint, 0644);
MODULE_PARM_DESC(default_active_lanes, "default active dispatch lanes (0 = no default cap)");

static uint max_gateway_connections = NAMRBD_MAX_PATHS;
module_param(max_gateway_connections, uint, 0644);
MODULE_PARM_DESC(max_gateway_connections, "maximum active dispatch lanes/gateway connections");

static uint per_path_outstanding = 16;
module_param(per_path_outstanding, uint, 0644);
MODULE_PARM_DESC(per_path_outstanding, "maximum outstanding requests per gateway path on one persistent connection");

static char *sched_policy = "least_inflight";
module_param(sched_policy, charp, 0644);
MODULE_PARM_DESC(sched_policy, "scheduler policy: rr|least_inflight|ewma");

static ulong down_mask;
module_param(down_mask, ulong, 0644);
MODULE_PARM_DESC(down_mask, "bitmask of path ids in DOWN state");

static ulong degraded_mask;
module_param(degraded_mask, ulong, 0644);
MODULE_PARM_DESC(degraded_mask, "bitmask of path ids in DEGRADED state");

static ulong draining_mask;
module_param(draining_mask, ulong, 0644);
MODULE_PARM_DESC(draining_mask, "bitmask of path ids in DRAINING state");

static int fail_path_id = -1;
module_param(fail_path_id, int, 0644);
MODULE_PARM_DESC(fail_path_id, "default injected path failure id for newly created devices");

static char *no_path_retry = "fail";
module_param(no_path_retry, charp, 0644);
MODULE_PARM_DESC(no_path_retry, "no-path retry policy: fail|queue|<seconds> (default: fail)");

static uint no_path_requeue_delay_ms = 1000;
module_param(no_path_requeue_delay_ms, uint, 0644);
MODULE_PARM_DESC(no_path_requeue_delay_ms, "delay before rechecking queued no-path requests in milliseconds");

static uint no_path_max_queued_requests;
module_param(no_path_max_queued_requests, uint, 0644);
MODULE_PARM_DESC(no_path_max_queued_requests, "maximum queued no-path requests (0 = unlimited)");

static bool trace_enabled;
module_param(trace_enabled, bool, 0644);
MODULE_PARM_DESC(trace_enabled, "enable verbose namrbd debug hooks");

static uint data_max_io_size = NAMRBD_DEFAULT_MAX_DATA_IO_SIZE;
module_param(data_max_io_size, uint, 0644);
MODULE_PARM_DESC(data_max_io_size, "maximum payload-bearing READ/WRITE request size in bytes");

enum namrbd_path_state {
	NAMRBD_PATH_UP = 0,
	NAMRBD_PATH_DEGRADED = 1,
	NAMRBD_PATH_DOWN = 2,
	NAMRBD_PATH_DRAINING = 3,
};

enum namrbd_sched_policy {
	NAMRBD_SCHED_RR = 0,
	NAMRBD_SCHED_LEAST_INFLIGHT = 1,
	NAMRBD_SCHED_EWMA = 2,
};

enum namrbd_no_path_reason {
	NAMRBD_NO_PATH_NONE = 0,
	NAMRBD_NO_PATH_DETACHED = 1,
	NAMRBD_NO_PATH_PLAN_EMPTY = 2,
	NAMRBD_NO_PATH_ALL_DOWN = 3,
	NAMRBD_NO_PATH_ALL_DRAINING = 4,
	NAMRBD_NO_PATH_NO_ELIGIBLE = 5,
	NAMRBD_NO_PATH_EXHAUSTED_AFTER_RETRY = 6,
};

enum namrbd_no_path_retry_mode {
	NAMRBD_NO_PATH_RETRY_FAIL = 0,
	NAMRBD_NO_PATH_RETRY_QUEUE = 1,
	NAMRBD_NO_PATH_RETRY_TIMED = 2,
};

enum namrbd_no_path_state {
	NAMRBD_NO_PATH_INACTIVE = 0,
	NAMRBD_NO_PATH_QUEUEING = 1,
	NAMRBD_NO_PATH_RECOVERING = 2,
	NAMRBD_NO_PATH_FAILING = 3,
};

struct namrbd_pending_req {
	struct list_head list;
	struct completion done;
	struct namrbd_blk_dev *dev;
	struct namrbd_path *path;
	struct request *rq;
	u64 request_id;
	u64 volume_id;
	u64 generation;
	u64 start_ns;
	u32 lane_id;
	u32 expected_op;
	namrbd_req_op_t op;
	u32 max_resp_len;
	ulong tried_mask;
	u32 attempt;
	int err;
	u32 wire_status;
	bool async;
	bool retry;
	bool sem_acquired;
	bool accounted;
	u64 accounted_bytes;
	bool completed;
	bool processing;
};

struct namrbd_path {
	u32 path_id;
	u32 priority;
	char gateway_id[NAMRBD_TRANSPORT_GATEWAY_ID_LEN];
	struct namrbd_transport_endpoint endpoint;
	enum namrbd_path_state state;
	enum namrbd_path_state configured_state;
	atomic_t inflight;
	u32 consecutive_errors;
	u32 last_errno;
	u32 last_wire_status;
	u64 state_changes;
	unsigned long last_transition_jiffies;
	u64 ewma_latency_ns;
	u64 completed;
	u64 retries;
	struct socket *sock;
	struct mutex io_lock;
	struct task_struct *recv_task;
	struct semaphore outstanding_sem;
	spinlock_t pending_lock;
	struct list_head pending_reqs;
	u32 outstanding_limit;
	u32 pending_high_water;
	u64 submitted;
	u64 connection_opens;
	u64 connection_resets;
	spinlock_t lock;
};

struct namrbd_resource_requeued_req {
	struct list_head list;
	struct request *rq;
};

#define NAMRBD_PATH_ID_NONE U32_MAX
#define NAMRBD_LANE_ID_NONE U32_MAX

struct namrbd_blk_dev {
	struct list_head list;
	struct mutex state_lock;
	u32 device_id;
	u32 disk_index;
	char disk_name[DISK_NAME_LEN];
	int fail_path_id;
	bool attached;
	u64 volume_id;
	u64 generation;
	u64 applied_path_plan_revision;
	u64 size_bytes;
	u32 chunk_size_bytes;
	char attached_host_id[NAMRBD_MAX_HOST_ID_LEN];
	u8 *data;
	spinlock_t data_lock;
	unsigned long *zero_map;
	u64 zero_map_granules;
	u32 zero_map_granule_bytes;
	spinlock_t zero_map_lock;
	u32 max_io_size;
	u32 max_data_io_size;
	u32 max_zero_like_io_size;
	u32 max_inflight_requests;
	u64 max_inflight_bytes;
	/* Phase C3: wire v2 dataplane auth (token-hmac-v1 when set) */
	char dataplane_auth_mode[32];
	char dataplane_token[2048];
	char dataplane_session_key[256];
	atomic_t data_inflight_reqs;
	atomic64_t data_inflight_bytes;
	atomic_t data_resource_requeued_reqs;
	atomic64_t data_resource_requeue_events;
	spinlock_t data_resource_requeue_lock;
	struct list_head data_resource_requeue_list;
	atomic64_t request_seq;
	struct delayed_work probe_work;
	u32 rr_cursor;
	enum namrbd_sched_policy policy;
	u32 active_lane_count;
	u32 target_nr_hw_queues;
	u64 queue_topology_generation;
	char queue_topology_state[24];
	u32 lane_preferred_path_ids[NAMRBD_MAX_PATHS];
	atomic64_t lane_dispatch_reqs[NAMRBD_MAX_PATHS];
	u64 lane_remap_count;
	u32 last_lane_remapped_lanes;
	unsigned long last_lane_remap_jiffies;
	char last_lane_remap_reason[32];
	u32 last_selected_lane_id;
	u32 last_selected_path_id;
	u32 last_completed_path_id;
	u32 last_failed_path_id;
	u32 last_failover_from_path_id;
	u32 last_failover_to_path_id;
	u32 last_no_path_reason;
	u32 last_no_path_op;
	u32 last_no_path_eligible_paths;
	u64 last_no_path_tried_mask;
	unsigned long last_no_path_jiffies;
	enum namrbd_no_path_retry_mode no_path_retry_mode;
	u32 no_path_retry_seconds;
	enum namrbd_no_path_state no_path_state;
	unsigned long no_path_since_jiffies;
	unsigned long no_path_retry_deadline_jiffies;
	unsigned long last_no_path_wakeup_jiffies;
	struct namrbd_path *paths;
	int nr_paths;
	u32 active_path_count;
	atomic64_t total_reqs;
	atomic64_t read_reqs;
	atomic64_t write_reqs;
	atomic64_t discard_reqs;
	atomic64_t write_zeroes_reqs;
	atomic64_t zero_map_local_skips;
	atomic64_t zero_map_mark_zero_reqs;
	atomic64_t zero_map_mark_data_reqs;
	atomic64_t retry_reqs;
	atomic64_t timeout_reqs;
	atomic64_t failed_reqs;
	atomic64_t no_path_reqs;
	atomic64_t no_path_queued_reqs;
	atomic64_t no_path_requeued_reqs;
	atomic64_t no_path_failed_reqs;
	atomic64_t no_path_recovered_reqs;
	atomic64_t no_path_enter_count;
	atomic64_t completed_reqs;
	atomic64_t path_failover_reqs;
	atomic64_t probe_failures;
	atomic64_t path_state_changes;
	atomic64_t last_errno;
	struct dentry *debugfs_dir;

	struct blk_mq_tag_set tag_set;
	struct request_queue *queue;
	struct gendisk *disk;
};

struct namrbd_blk_mgr {
	struct mutex lock;
	struct idr devices;
	struct ida disk_indexes;
	struct list_head device_list;
	struct dentry *debugfs_root;
	struct dentry *debugfs_devices_root;
	int major;
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
	} paths[NAMRBD_MAX_PATHS];
	struct {
		u32 lane_id;
		u32 preferred_path_id;
		u32 fallback_path_id;
		u32 readiness;
		u64 dispatch_reqs;
	} lanes[NAMRBD_MAX_PATHS];
};

enum namrbd_lane_readiness {
	NAMRBD_LANE_READY_UNSPEC = 0,
	NAMRBD_LANE_READY_STABLE = 1,
	NAMRBD_LANE_READY_DEGRADED_WITH_UP_FALLBACK = 2,
	NAMRBD_LANE_READY_DEGRADED_WITHOUT_UP_FALLBACK = 3,
	NAMRBD_LANE_READY_UNAVAILABLE = 4,
};

static struct namrbd_blk_mgr g_mgr = {
	.lock = __MUTEX_INITIALIZER(g_mgr.lock),
	.devices = IDR_INIT(g_mgr.devices),
	.disk_indexes = IDA_INIT(g_mgr.disk_indexes),
	.device_list = LIST_HEAD_INIT(g_mgr.device_list),
	.major = 0,
};

/*
 * Legacy exported APIs remain for compatibility with Phase C control-path code.
 * New Phase C1 entry points add explicit device_id-aware lifecycle handling.
 */
int namrbd_blk_create(u32 *device_id_out);
int namrbd_blk_destroy(u32 device_id);
int namrbd_blk_activate_device(u32 device_id, u64 volume_id, u64 size_bytes,
			       u32 block_size, u32 chunk_size_bytes, u64 generation);
int namrbd_blk_activate_device_with_initial_zero_map(u32 device_id, u64 volume_id,
						     u64 size_bytes, u32 block_size,
						     u32 chunk_size_bytes, u64 generation,
						     bool initial_zero_map_all_zero);
int namrbd_blk_resize_device(u32 device_id, u64 volume_id, u64 generation, u64 size_bytes);
void namrbd_blk_deactivate_device(u32 device_id, u64 volume_id);
int namrbd_blk_configure_data_paths_device(u32 device_id,
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
int namrbd_blk_configure_data_path_device(u32 device_id, const char *address, u16 port,
					  u32 dataplane_path_count,
					  u32 max_inflight_requests,
					  u64 max_inflight_bytes,
					  u32 max_io_size,
					  u32 max_zero_like_io_size,
					  const char *host_id,
					  const char *dataplane_auth_mode,
					  const char *dataplane_token,
					  const char *dataplane_session_key);
int namrbd_blk_get_status(u32 device_id, struct namrbd_blk_status *out);
int namrbd_blk_update_path_masks_device(u32 device_id, u64 path_plan_revision,
					u64 down_mask_bits, u64 degraded_mask_bits, u64 draining_mask_bits);
int namrbd_blk_list_devices(struct namrbd_blk_status *out, u32 max_entries, u32 *count_out);

int namrbd_blk_activate(u64 volume_id, u64 size_bytes, u32 block_size,
			u32 chunk_size_bytes, u64 generation);
int namrbd_blk_resize(u64 volume_id, u64 generation, u64 size_bytes);
void namrbd_blk_deactivate(u64 volume_id);
int namrbd_blk_configure_data_path(const char *address, u16 port,
				   u32 max_inflight_requests,
				   u64 max_inflight_bytes,
				   u32 max_io_size);

static blk_status_t namrbd_rw_request(struct namrbd_blk_dev *dev, struct request *rq,
				      struct namrbd_path *path);
static int namrbd_probe_data_path(struct namrbd_blk_dev *dev);

static const struct namrbd_transport_endpoint *
namrbd_transport_endpoint_for_path(struct namrbd_path *path)
{
	if (!path)
		return NULL;
	if (!path->endpoint.port || !path->endpoint.address[0])
		return NULL;
	return &path->endpoint;
}

static bool namrbd_device_has_dataplane_endpoint(struct namrbd_blk_dev *dev)
{
	int i;

	if (!dev || !dev->paths)
		return false;
	for (i = 0; i < dev->nr_paths; i++) {
		if (namrbd_transport_endpoint_for_path(&dev->paths[i]))
			return true;
	}
	return false;
}

static u32 namrbd_per_path_outstanding_limit(void)
{
	if (per_path_outstanding < 1)
		return 1;
	if (per_path_outstanding > NAMRBD_QUEUE_DEPTH)
		return NAMRBD_QUEUE_DEPTH;
	return per_path_outstanding;
}

static int namrbd_path_recv_worker(void *arg);
static void namrbd_path_fail_pending(struct namrbd_path *path, int err);
static void namrbd_finish_async_pending(struct namrbd_pending_req *pending,
					int err, u32 wire_status,
					bool already_removed);
static int namrbd_data_path_submit_async(struct namrbd_blk_dev *dev, struct request *rq,
					 struct namrbd_path *path, u32 lane_id,
					 bool retry, ulong tried_mask, u32 attempt);

static void namrbd_path_detach_socket_locked(struct namrbd_path *path,
					     struct socket **sock_out,
					     struct task_struct **task_out)
{
	struct socket *sock;
	struct task_struct *task;

	*sock_out = NULL;
	*task_out = NULL;
	if (!path)
		return;
	sock = READ_ONCE(path->sock);
	task = path->recv_task;
	path->recv_task = NULL;
	WRITE_ONCE(path->sock, NULL);
	if (sock) {
		kernel_sock_shutdown(sock, SHUT_RDWR);
		path->connection_resets++;
		*sock_out = sock;
	}
	*task_out = task;
}

static void namrbd_path_finish_socket_close(struct socket *sock, struct task_struct *task)
{
	if (task) {
		if (task != current)
			kthread_stop(task);
		put_task_struct(task);
	}
	if (sock)
		sock_release(sock);
}

static void namrbd_path_close_socket(struct namrbd_path *path)
{
	struct socket *sock = NULL;
	struct task_struct *task = NULL;

	if (!path)
		return;
	mutex_lock(&path->io_lock);
	namrbd_path_detach_socket_locked(path, &sock, &task);
	mutex_unlock(&path->io_lock);
	namrbd_path_fail_pending(path, -EIO);
	namrbd_path_finish_socket_close(sock, task);
}

static int namrbd_path_ensure_socket_locked(struct namrbd_path *path)
{
	struct socket *sock = NULL;
	struct task_struct *task;
	int ret;

	if (!path)
		return -EINVAL;
	if (READ_ONCE(path->sock))
		return 0;
	ret = namrbd_transport_connect(namrbd_transport_endpoint_for_path(path), &sock);
	if (ret < 0)
		return ret;
	task = kthread_create(namrbd_path_recv_worker, path, "namrbd-rx-%u", path->path_id);
	if (IS_ERR(task)) {
		ret = PTR_ERR(task);
		sock_release(sock);
		return ret;
	}
	get_task_struct(task);
	path->recv_task = task;
	WRITE_ONCE(path->sock, sock);
	wake_up_process(task);
	path->connection_opens++;
	return 0;
}

static int namrbd_path_slot_by_id(struct namrbd_blk_dev *dev, u32 path_id)
{
	u32 i;

	if (!dev || !dev->paths)
		return -ENOENT;
	for (i = 0; i < (u32)dev->nr_paths; i++) {
		if (dev->paths[i].path_id == path_id)
			return (int)i;
	}
	return -ENOENT;
}

static int namrbd_validate_transport_paths(const struct namrbd_transport_path *paths,
					   u32 dataplane_path_count,
					   u32 max_paths)
{
	u32 i;
	u32 j;

	if (!paths || !dataplane_path_count)
		return -EINVAL;
	if (dataplane_path_count > max_paths)
		return -E2BIG;
	for (i = 0; i < dataplane_path_count; i++) {
		if (!paths[i].endpoint.address[0] || !paths[i].endpoint.port)
			return -EINVAL;
		for (j = 0; j < i; j++) {
			if (paths[i].path_id == paths[j].path_id)
				return -EEXIST;
		}
	}
	return 0;
}

static const struct block_device_operations namrbd_fops = {
	.owner = THIS_MODULE,
};

static const char *namrbd_sched_policy_str(enum namrbd_sched_policy policy)
{
	switch (policy) {
	case NAMRBD_SCHED_RR:
		return "rr";
	case NAMRBD_SCHED_EWMA:
		return "ewma";
	default:
		return "least_inflight";
	}
}

static u32 namrbd_count_lane_eligible_paths(struct namrbd_blk_dev *dev)
{
	u32 eligible = 0;
	u32 limit;
	u32 i;

	if (!dev || !dev->paths)
		return 0;
	limit = dev->active_path_count ? dev->active_path_count : dev->nr_paths;
	for (i = 0; i < limit; i++) {
		switch (dev->paths[i].state) {
		case NAMRBD_PATH_UP:
		case NAMRBD_PATH_DEGRADED:
			eligible++;
			break;
		default:
			break;
		}
	}
	return eligible;
}

static bool namrbd_path_id_lane_eligible(struct namrbd_blk_dev *dev, u32 path_id)
{
	int slot;

	if (!dev || !dev->paths)
		return false;
	slot = namrbd_path_slot_by_id(dev, path_id);
	if (slot < 0)
		return false;
	return dev->paths[slot].state == NAMRBD_PATH_UP ||
	       dev->paths[slot].state == NAMRBD_PATH_DEGRADED;
}

static u32 namrbd_compute_active_lane_count(struct namrbd_blk_dev *dev)
{
	u32 lanes;
	u32 online_cpus;

	lanes = namrbd_count_lane_eligible_paths(dev);
	if (lanes == 0)
		return 0;
	online_cpus = num_online_cpus();
	if (online_cpus > 0 && lanes > online_cpus)
		lanes = online_cpus;
	if (max_gateway_connections > 0 && lanes > max_gateway_connections)
		lanes = max_gateway_connections;
	if (default_active_lanes > 0 && lanes > default_active_lanes)
		lanes = default_active_lanes;
	return lanes;
}

static void namrbd_refresh_active_lane_count(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return;
	dev->active_lane_count = namrbd_compute_active_lane_count(dev);
}

static u32 namrbd_compute_target_nr_hw_queues(struct namrbd_blk_dev *dev)
{
	u32 target;
	u32 online_cpus;

	if (!dev)
		return 1;

	target = dev->active_lane_count;
	if (target == 0)
		target = 1;

	online_cpus = num_online_cpus();
	if (online_cpus == 0)
		online_cpus = 1;
	if (target > online_cpus)
		target = online_cpus;
	if (max_gateway_connections > 0 && target > max_gateway_connections)
		target = max_gateway_connections;
	if (target == 0)
		target = 1;
	return target;
}

static void namrbd_refresh_target_nr_hw_queues(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return;
	dev->target_nr_hw_queues = namrbd_compute_target_nr_hw_queues(dev);
}

static const char *namrbd_queue_topology_state_str(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return "unknown";
	if (dev->tag_set.nr_hw_queues == dev->target_nr_hw_queues)
		return "stable";
	return "planned";
}

/*
 * Queue topology target is a control-path concern. We intentionally do not
 * recompute it from fast-path I/O completion or periodic probe churn, because
 * queue-count changes must stay aligned with low-frequency attach/reconfigure
 * events and must not flap on transient path-state transitions.
 */
static void namrbd_refresh_queue_topology_target_control(struct namrbd_blk_dev *dev)
{
	u32 prev_target;
	char prev_state[sizeof(dev->queue_topology_state)];
	const char *state;

	if (!dev)
		return;

	prev_target = dev->target_nr_hw_queues;
	strscpy(prev_state, dev->queue_topology_state, sizeof(prev_state));
	namrbd_refresh_target_nr_hw_queues(dev);
	state = namrbd_queue_topology_state_str(dev);
	if (prev_target != dev->target_nr_hw_queues ||
	    strncmp(prev_state, state, sizeof(prev_state)) != 0)
		dev->queue_topology_generation++;
	strscpy(dev->queue_topology_state, state, sizeof(dev->queue_topology_state));
}

static int namrbd_apply_queue_topology_control(struct namrbd_blk_dev *dev,
					       const char *reason)
{
	u32 target;
	int ret = 0;

	if (!dev || !dev->queue)
		return 0;

	target = dev->target_nr_hw_queues;
	if (target == 0)
		target = 1;
	if (dev->tag_set.nr_hw_queues == target) {
		namrbd_refresh_queue_topology_target_control(dev);
		return 0;
	}
	if (dev->no_path_state == NAMRBD_NO_PATH_QUEUEING ||
	    dev->no_path_state == NAMRBD_NO_PATH_RECOVERING) {
		/*
		 * Replacement/no-path recovery can reconfigure datapaths while
		 * requests are intentionally stuck on the blk-mq requeue list.
		 * Quiescing the queue here can stall that recovery, so keep the
		 * target in "planned" state and let probe work reconcile it once
		 * requests flow again.
		 */
		namrbd_refresh_queue_topology_target_control(dev);
		return 0;
	}

#if NAMRBD_HAVE_BLK_MQ_UPDATE_NR_HW_QUEUES
	blk_mq_quiesce_queue(dev->queue);
	blk_mq_update_nr_hw_queues(&dev->tag_set, target);
	blk_mq_unquiesce_queue(dev->queue);
	namrbd_refresh_queue_topology_target_control(dev);
#else
	ret = -EOPNOTSUPP;
#endif
	if (ret) {
		pr_warn("namrbd_blk: device_id=%u queue-topology apply failed reason=%s applied=%u target=%u err=%d\n",
			dev->device_id, reason ? reason : "unknown",
			dev->tag_set.nr_hw_queues, target, ret);
		return ret;
	}

	pr_info("namrbd_blk: device_id=%u queue-topology applied reason=%s nr_hw_queues=%u target=%u generation=%llu\n",
		dev->device_id, reason ? reason : "unknown",
		dev->tag_set.nr_hw_queues, dev->target_nr_hw_queues,
		(unsigned long long)dev->queue_topology_generation);
	return 0;
}

static void namrbd_refresh_lane_map(struct namrbd_blk_dev *dev, const char *reason)
{
	u32 previous_lane_count;
	u32 next_lane_count;
	u32 next_ids[NAMRBD_MAX_PATHS];
	bool used_slots[NAMRBD_MAX_PATHS];
	bool changed = false;
	u32 remapped_lanes = 0;
	u32 i;

	if (!dev)
		return;

	previous_lane_count = dev->active_lane_count;
	namrbd_refresh_active_lane_count(dev);
	next_lane_count = dev->active_lane_count;
	for (i = 0; i < NAMRBD_MAX_PATHS; i++) {
		next_ids[i] = NAMRBD_PATH_ID_NONE;
		used_slots[i] = false;
	}

	/*
	 * Preserve surviving preferred paths first so that unaffected lanes keep
	 * their ordering domain affinity across transient path changes.
	 */
	for (i = 0; i < next_lane_count; i++) {
		u32 prev = dev->lane_preferred_path_ids[i];
		int slot;

		if (prev == NAMRBD_PATH_ID_NONE)
			continue;
		if (!namrbd_path_id_lane_eligible(dev, prev))
			continue;
		slot = namrbd_path_slot_by_id(dev, prev);
		if (slot < 0)
			continue;
		if (used_slots[slot])
			continue;
		next_ids[i] = prev;
		used_slots[slot] = true;
	}

	/* Fill any remaining lanes from the current eligible path set. */
	for (i = 0; i < next_lane_count; i++) {
		u32 candidate;

		if (next_ids[i] != NAMRBD_PATH_ID_NONE)
			continue;
		for (candidate = 0; candidate < (dev->active_path_count ? dev->active_path_count : dev->nr_paths); candidate++) {
			u32 candidate_path_id = dev->paths[candidate].path_id;

			if (!namrbd_path_id_lane_eligible(dev, candidate_path_id))
				continue;
			if (used_slots[candidate])
				continue;
			next_ids[i] = candidate_path_id;
			used_slots[candidate] = true;
			break;
		}
	}

	if (previous_lane_count != next_lane_count)
		changed = true;
	for (i = 0; i < NAMRBD_MAX_PATHS; i++) {
		if (dev->lane_preferred_path_ids[i] != next_ids[i]) {
			changed = true;
			if (i < next_lane_count || i < previous_lane_count)
				remapped_lanes++;
		}
	}
	for (i = 0; i < NAMRBD_MAX_PATHS; i++)
		dev->lane_preferred_path_ids[i] = next_ids[i];
	if (changed) {
		dev->lane_remap_count++;
		dev->last_lane_remapped_lanes = remapped_lanes;
		dev->last_lane_remap_jiffies = jiffies;
		strscpy(dev->last_lane_remap_reason, reason ? reason : "unknown",
			sizeof(dev->last_lane_remap_reason));
	}
}

static const char *namrbd_lane_remap_reason_for_path_state(enum namrbd_path_state old_state,
							  enum namrbd_path_state new_state)
{
	if (new_state == NAMRBD_PATH_DOWN)
		return "path_down";
	if (new_state == NAMRBD_PATH_DEGRADED)
		return "path_degraded";
	if (new_state == NAMRBD_PATH_DRAINING)
		return "path_draining";
	if (new_state == NAMRBD_PATH_UP && old_state != NAMRBD_PATH_UP)
		return "path_recovered";
	return "path_state_change";
}

static enum namrbd_path_state namrbd_path_id_state(struct namrbd_blk_dev *dev, u32 path_id)
{
	int slot;

	if (!dev || !dev->paths)
		return NAMRBD_PATH_DOWN;
	slot = namrbd_path_slot_by_id(dev, path_id);
	if (slot < 0)
		return NAMRBD_PATH_DOWN;
	return dev->paths[slot].state;
}

static u32 namrbd_lane_fallback_path_id(struct namrbd_blk_dev *dev, u32 preferred_path_id)
{
	u32 i;
	bool prefer_up_only = false;

	if (preferred_path_id != NAMRBD_PATH_ID_NONE &&
	    namrbd_path_id_state(dev, preferred_path_id) == NAMRBD_PATH_DEGRADED)
		prefer_up_only = true;

	if (!dev || !dev->paths)
		return NAMRBD_PATH_ID_NONE;
	if (prefer_up_only) {
		for (i = 0; i < (u32)dev->nr_paths; i++) {
			if (dev->paths[i].state != NAMRBD_PATH_UP)
				continue;
			if (dev->paths[i].path_id == preferred_path_id)
				continue;
			return dev->paths[i].path_id;
		}
	}
	for (i = 0; i < (u32)dev->nr_paths; i++) {
		if (dev->paths[i].state != NAMRBD_PATH_UP &&
		    dev->paths[i].state != NAMRBD_PATH_DEGRADED)
			continue;
		if (dev->paths[i].path_id == preferred_path_id)
			continue;
		return dev->paths[i].path_id;
	}
	return NAMRBD_PATH_ID_NONE;
}

static u32 namrbd_lane_readiness(struct namrbd_blk_dev *dev, u32 preferred_path_id,
				 u32 fallback_path_id)
{
	enum namrbd_path_state preferred_state;
	enum namrbd_path_state fallback_state;

	if (preferred_path_id == NAMRBD_PATH_ID_NONE)
		return NAMRBD_LANE_READY_UNAVAILABLE;
	preferred_state = namrbd_path_id_state(dev, preferred_path_id);
	if (preferred_state == NAMRBD_PATH_UP)
		return NAMRBD_LANE_READY_STABLE;
	if (preferred_state == NAMRBD_PATH_DEGRADED) {
		if (fallback_path_id == NAMRBD_PATH_ID_NONE)
			return NAMRBD_LANE_READY_DEGRADED_WITHOUT_UP_FALLBACK;
		fallback_state = namrbd_path_id_state(dev, fallback_path_id);
		if (fallback_state == NAMRBD_PATH_UP)
			return NAMRBD_LANE_READY_DEGRADED_WITH_UP_FALLBACK;
		return NAMRBD_LANE_READY_DEGRADED_WITHOUT_UP_FALLBACK;
	}
	return NAMRBD_LANE_READY_UNAVAILABLE;
}

static const char *namrbd_lane_readiness_str(u32 readiness)
{
	switch (readiness) {
	case NAMRBD_LANE_READY_STABLE:
		return "stable";
	case NAMRBD_LANE_READY_DEGRADED_WITH_UP_FALLBACK:
		return "degraded_with_up_fallback";
	case NAMRBD_LANE_READY_DEGRADED_WITHOUT_UP_FALLBACK:
		return "degraded_without_up_fallback";
	case NAMRBD_LANE_READY_UNAVAILABLE:
		return "unavailable";
	default:
		return "unknown";
	}
}

#define NAMRBD_TRACE(fmt, ...)                             \
	do {                                               \
		if (trace_enabled)                         \
			pr_debug("namrbd: " fmt, ##__VA_ARGS__); \
	} while (0)

#define NAMRBD_TRACE_INFO(fmt, ...)                              \
	do {                                                     \
		if (trace_enabled)                               \
			pr_info_ratelimited("namrbd_blk: " fmt,  \
					    ##__VA_ARGS__);        \
	} while (0)

static const char *namrbd_path_state_str(enum namrbd_path_state s)
{
	switch (s) {
	case NAMRBD_PATH_UP:
		return "UP";
	case NAMRBD_PATH_DEGRADED:
		return "DEGRADED";
	case NAMRBD_PATH_DOWN:
		return "DOWN";
	case NAMRBD_PATH_DRAINING:
		return "DRAINING";
	default:
		return "UNKNOWN";
	}
}

static const char *namrbd_wire_status_str(u32 status)
{
	switch ((s32)status) {
	case 0:
		return "ok";
	case 1:
		return "bad_magic";
	case 2:
		return "unsupported_version";
	case 3:
		return "unauthorized";
	case 4:
		return "no_such_volume";
	case 5:
		return "generation_mismatch";
	case 6:
		return "invalid_range";
	case 7:
		return "path_draining";
	case 8:
		return "no_healthy_replica";
	case 9:
		return "quorum_failed";
	case 10:
		return "timeout";
	case 11:
		return "retryable";
	case 12:
		return "busy";
	case 13:
		return "checksum";
	case 14:
		return "internal";
	default:
		return "unknown";
	}
}

static struct namrbd_blk_dev *namrbd_dev_from_disk_device(struct device *d)
{
	struct gendisk *disk;

	if (!d)
		return NULL;
	disk = dev_to_disk(d);
	if (!disk)
		return NULL;
	return disk->private_data;
}

static struct namrbd_blk_dev *namrbd_blk_lookup_device_locked(u32 device_id)
{
	return idr_find(&g_mgr.devices, device_id);
}

static struct namrbd_blk_dev *namrbd_blk_lookup_device(u32 device_id)
{
	struct namrbd_blk_dev *dev;

	mutex_lock(&g_mgr.lock);
	dev = namrbd_blk_lookup_device_locked(device_id);
	mutex_unlock(&g_mgr.lock);
	return dev;
}

static struct namrbd_blk_dev *namrbd_blk_lookup_default_device(void)
{
	struct namrbd_blk_dev *dev = NULL;

	mutex_lock(&g_mgr.lock);
	dev = namrbd_blk_lookup_device_locked(0);
	mutex_unlock(&g_mgr.lock);
	return dev;
}

static void namrbd_wake_no_path_queue(struct namrbd_blk_dev *dev, const char *reason);

static void namrbd_path_transition(struct namrbd_blk_dev *dev, struct namrbd_path *p,
				   enum namrbd_path_state new_state, u32 err,
				   u32 wire_status)
{
	if (p->state == new_state && p->last_errno == err &&
	    p->last_wire_status == wire_status)
		return;

	NAMRBD_TRACE("device=%u path_transition id=%u %s->%s errno=%u wire=%u\n",
		     dev->device_id, p->path_id, namrbd_path_state_str(p->state),
		     namrbd_path_state_str(new_state), err, wire_status);
	p->state = new_state;
	p->last_errno = err;
	p->last_wire_status = wire_status;
	p->state_changes++;
	p->last_transition_jiffies = jiffies;
	atomic64_inc(&dev->path_state_changes);
}

static void namrbd_path_mark_failure(struct namrbd_blk_dev *dev, struct namrbd_path *p,
				     u32 err, u32 wire_status)
{
	unsigned long flags;
	enum namrbd_path_state old_state;
	enum namrbd_path_state new_state;

	spin_lock_irqsave(&p->lock, flags);
	old_state = p->state;
	p->consecutive_errors++;
	if (p->consecutive_errors >= NAMRBD_PATH_DOWN_THRESHOLD)
		namrbd_path_transition(dev, p, NAMRBD_PATH_DOWN, err, wire_status);
	else if (p->consecutive_errors >= NAMRBD_PATH_DEGRADE_THRESHOLD)
		namrbd_path_transition(dev, p, NAMRBD_PATH_DEGRADED, err, wire_status);
	new_state = p->state;
	spin_unlock_irqrestore(&p->lock, flags);
	atomic64_set(&dev->last_errno, err);
	if (new_state != old_state)
		namrbd_refresh_lane_map(dev,
			namrbd_lane_remap_reason_for_path_state(old_state, new_state));
	if (new_state != old_state &&
	    (new_state == NAMRBD_PATH_UP || new_state == NAMRBD_PATH_DEGRADED))
		namrbd_wake_no_path_queue(dev, "path_failure_transition");
}

static void namrbd_path_mark_success(struct namrbd_blk_dev *dev, struct namrbd_path *p)
{
	unsigned long flags;
	enum namrbd_path_state old_state;
	enum namrbd_path_state new_state;

	spin_lock_irqsave(&p->lock, flags);
	old_state = p->state;
	p->consecutive_errors = 0;
	if (p->configured_state == NAMRBD_PATH_UP)
		namrbd_path_transition(dev, p, NAMRBD_PATH_UP, 0, 0);
	else if (p->state != p->configured_state)
		namrbd_path_transition(dev, p, p->configured_state, 0, 0);
	new_state = p->state;
	spin_unlock_irqrestore(&p->lock, flags);
	if (new_state != old_state)
		namrbd_refresh_lane_map(dev,
			namrbd_lane_remap_reason_for_path_state(old_state, new_state));
	if (new_state != old_state &&
	    (new_state == NAMRBD_PATH_UP || new_state == NAMRBD_PATH_DEGRADED))
		namrbd_wake_no_path_queue(dev, "path_success_transition");
}

static bool namrbd_has_up_path(struct namrbd_blk_dev *dev, ulong tried_mask)
{
	int i;

	for (i = 0; i < dev->nr_paths; i++) {
		if (tried_mask & (1UL << i))
			continue;
		if (dev->paths[i].state == NAMRBD_PATH_UP)
			return true;
	}
	return false;
}

static bool namrbd_op_is_dataplane(namrbd_req_op_t op)
{
	return op == REQ_OP_READ || op == REQ_OP_WRITE ||
	       op == REQ_OP_DISCARD || op == REQ_OP_WRITE_ZEROES;
}

static bool namrbd_op_is_zero_like(namrbd_req_op_t op)
{
	return op == REQ_OP_DISCARD || op == REQ_OP_WRITE_ZEROES;
}

static u64 namrbd_request_inflight_bytes(struct request *rq, namrbd_req_op_t op)
{
	if (!rq || namrbd_op_is_zero_like(op))
		return 0;
	return blk_rq_bytes(rq);
}

static void namrbd_zero_map_free(struct namrbd_blk_dev *dev)
{
	unsigned long *old;
	unsigned long flags;

	if (!dev)
		return;

	spin_lock_irqsave(&dev->zero_map_lock, flags);
	old = dev->zero_map;
	dev->zero_map = NULL;
	dev->zero_map_granules = 0;
	dev->zero_map_granule_bytes = 0;
	spin_unlock_irqrestore(&dev->zero_map_lock, flags);
	bitmap_free(old);
}

static int namrbd_zero_map_init(struct namrbd_blk_dev *dev, u64 size_bytes,
				bool all_zero)
{
	unsigned long *map;
	unsigned long *old;
	unsigned long flags;
	u64 granules;

	if (!dev || !size_bytes) {
		if (dev)
			namrbd_zero_map_free(dev);
		return 0;
	}

	granules = DIV_ROUND_UP_ULL(size_bytes, NAMRBD_ZERO_MAP_GRANULE_BYTES);
	if (!granules || granules > UINT_MAX) {
		namrbd_zero_map_free(dev);
		pr_warn("namrbd_blk: device_id=%u zero_map disabled size_bytes=%llu granules=%llu\n",
			dev->device_id, (unsigned long long)size_bytes,
			(unsigned long long)granules);
		return 0;
	}

	map = bitmap_zalloc((unsigned int)granules, GFP_KERNEL);
	if (!map)
		return -ENOMEM;
	if (all_zero)
		bitmap_fill(map, (unsigned int)granules);

	spin_lock_irqsave(&dev->zero_map_lock, flags);
	old = dev->zero_map;
	dev->zero_map = map;
	dev->zero_map_granules = granules;
	dev->zero_map_granule_bytes = NAMRBD_ZERO_MAP_GRANULE_BYTES;
	spin_unlock_irqrestore(&dev->zero_map_lock, flags);
	bitmap_free(old);

	pr_info("namrbd_blk: device_id=%u zero_map initialized granule_bytes=%u granules=%llu all_zero=%u\n",
		dev->device_id, NAMRBD_ZERO_MAP_GRANULE_BYTES,
		(unsigned long long)granules, all_zero ? 1 : 0);
	return 0;
}

static bool namrbd_zero_map_range_known_zero(struct namrbd_blk_dev *dev,
					     u64 offset_bytes, u64 length_bytes)
{
	unsigned long flags;
	unsigned long next_zero;
	u64 end_bytes;
	u64 start_granule;
	u64 end_granule;
	bool known_zero = false;

	if (!dev || !length_bytes)
		return false;
	end_bytes = offset_bytes + length_bytes;
	if (end_bytes < offset_bytes)
		return false;

	spin_lock_irqsave(&dev->zero_map_lock, flags);
	if (!dev->zero_map || !dev->zero_map_granules || !dev->zero_map_granule_bytes)
		goto out;
	start_granule = offset_bytes / dev->zero_map_granule_bytes;
	end_granule = DIV_ROUND_UP_ULL(end_bytes, dev->zero_map_granule_bytes);
	if (start_granule >= end_granule || end_granule > dev->zero_map_granules ||
	    end_granule > UINT_MAX)
		goto out;
	next_zero = find_next_zero_bit(dev->zero_map, (unsigned long)end_granule,
				       (unsigned long)start_granule);
	known_zero = next_zero >= (unsigned long)end_granule;
out:
	spin_unlock_irqrestore(&dev->zero_map_lock, flags);
	return known_zero;
}

static void namrbd_zero_map_mark_range(struct namrbd_blk_dev *dev,
				       u64 offset_bytes, u64 length_bytes,
				       bool zero)
{
	unsigned long flags;
	u64 end_bytes;
	u64 start_granule;
	u64 end_granule;
	u64 nr_granules;

	if (!dev || !length_bytes)
		return;
	end_bytes = offset_bytes + length_bytes;
	if (end_bytes < offset_bytes)
		return;

	spin_lock_irqsave(&dev->zero_map_lock, flags);
	if (!dev->zero_map || !dev->zero_map_granules || !dev->zero_map_granule_bytes)
		goto out;
	if (zero) {
		start_granule = DIV_ROUND_UP_ULL(offset_bytes, dev->zero_map_granule_bytes);
		end_granule = end_bytes / dev->zero_map_granule_bytes;
	} else {
		start_granule = offset_bytes / dev->zero_map_granule_bytes;
		end_granule = DIV_ROUND_UP_ULL(end_bytes, dev->zero_map_granule_bytes);
	}
	if (start_granule >= end_granule || start_granule >= dev->zero_map_granules)
		goto out;
	if (end_granule > dev->zero_map_granules)
		end_granule = dev->zero_map_granules;
	if (end_granule > UINT_MAX)
		goto out;
	nr_granules = end_granule - start_granule;
	if (!nr_granules || nr_granules > UINT_MAX)
		goto out;
	if (zero) {
		bitmap_set(dev->zero_map, (unsigned int)start_granule,
			   (unsigned int)nr_granules);
		atomic64_inc(&dev->zero_map_mark_zero_reqs);
	} else {
		bitmap_clear(dev->zero_map, (unsigned int)start_granule,
			     (unsigned int)nr_granules);
		atomic64_inc(&dev->zero_map_mark_data_reqs);
	}
out:
	spin_unlock_irqrestore(&dev->zero_map_lock, flags);
}

static bool namrbd_try_complete_zero_like_from_zero_map(struct namrbd_blk_dev *dev,
							struct request *rq,
							namrbd_req_op_t op)
{
	u64 offset_bytes;
	u64 length_bytes;

	if (!dev || !rq || !namrbd_op_is_zero_like(op))
		return false;
	offset_bytes = (u64)blk_rq_pos(rq) << NAMRBD_SECTOR_SHIFT;
	length_bytes = blk_rq_bytes(rq);
	if (!namrbd_zero_map_range_known_zero(dev, offset_bytes, length_bytes))
		return false;

	atomic64_inc(&dev->zero_map_local_skips);
	atomic64_inc(&dev->completed_reqs);
	NAMRBD_TRACE_INFO("device=%u zero_map_local_skip op=%u sector=%llu bytes=%u\n",
			  dev->device_id, op,
			  (unsigned long long)blk_rq_pos(rq), blk_rq_bytes(rq));
	blk_mq_end_request(rq, BLK_STS_OK);
	return true;
}

static void namrbd_zero_map_update_after_success(struct namrbd_blk_dev *dev,
						 struct request *rq,
						 namrbd_req_op_t op)
{
	u64 offset_bytes;
	u64 length_bytes;

	if (!dev || !rq)
		return;
	if (op != REQ_OP_WRITE && !namrbd_op_is_zero_like(op))
		return;
	offset_bytes = (u64)blk_rq_pos(rq) << NAMRBD_SECTOR_SHIFT;
	length_bytes = blk_rq_bytes(rq);
	if (op == REQ_OP_WRITE)
		namrbd_zero_map_mark_range(dev, offset_bytes, length_bytes, false);
	else
		namrbd_zero_map_mark_range(dev, offset_bytes, length_bytes, true);
}

static bool namrbd_op_is_mutating(namrbd_req_op_t op)
{
	return op == REQ_OP_WRITE || op == REQ_OP_DISCARD ||
	       op == REQ_OP_WRITE_ZEROES;
}

static bool namrbd_path_eligible(namrbd_req_op_t op, struct namrbd_path *p, bool prefer_up_only)
{
	if (p->state == NAMRBD_PATH_DOWN)
		return false;
	if (p->state == NAMRBD_PATH_DRAINING)
		return false;
	if (prefer_up_only && p->state != NAMRBD_PATH_UP)
		return false;
	if (namrbd_op_is_mutating(op) && prefer_up_only && p->state != NAMRBD_PATH_UP)
		return false;
	return true;
}

static const char *namrbd_no_path_reason_str(u32 reason)
{
	switch (reason) {
	case NAMRBD_NO_PATH_DETACHED:
		return "detached";
	case NAMRBD_NO_PATH_PLAN_EMPTY:
		return "path_plan_empty";
	case NAMRBD_NO_PATH_ALL_DOWN:
		return "all_paths_down";
	case NAMRBD_NO_PATH_ALL_DRAINING:
		return "all_paths_draining";
	case NAMRBD_NO_PATH_NO_ELIGIBLE:
		return "no_eligible_path";
	case NAMRBD_NO_PATH_EXHAUSTED_AFTER_RETRY:
		return "exhausted_after_retry";
	case NAMRBD_NO_PATH_NONE:
	default:
		return "none";
	}
}

static const char *namrbd_no_path_retry_mode_str(u32 mode)
{
	switch (mode) {
	case NAMRBD_NO_PATH_RETRY_QUEUE:
		return "queue";
	case NAMRBD_NO_PATH_RETRY_TIMED:
		return "timed";
	case NAMRBD_NO_PATH_RETRY_FAIL:
	default:
		return "fail";
	}
}

static const char *namrbd_no_path_state_str(u32 state)
{
	switch (state) {
	case NAMRBD_NO_PATH_QUEUEING:
		return "queueing";
	case NAMRBD_NO_PATH_RECOVERING:
		return "recovering";
	case NAMRBD_NO_PATH_FAILING:
		return "failing";
	case NAMRBD_NO_PATH_INACTIVE:
	default:
		return "inactive";
	}
}

static int namrbd_parse_no_path_retry(const char *raw,
				      enum namrbd_no_path_retry_mode *mode,
				      u32 *seconds)
{
	u32 parsed_seconds;

	if (!raw || !raw[0] || !strcmp(raw, "fail")) {
		*mode = NAMRBD_NO_PATH_RETRY_FAIL;
		*seconds = 0;
		return 0;
	}
	if (!strcmp(raw, "queue")) {
		*mode = NAMRBD_NO_PATH_RETRY_QUEUE;
		*seconds = 0;
		return 0;
	}
	if (kstrtou32(raw, 10, &parsed_seconds) || parsed_seconds == 0)
		return -EINVAL;
	*mode = NAMRBD_NO_PATH_RETRY_TIMED;
	*seconds = parsed_seconds;
	return 0;
}

static enum namrbd_no_path_reason namrbd_classify_no_path(struct namrbd_blk_dev *dev,
							  namrbd_req_op_t op,
							  ulong tried_mask,
							  u32 *eligible_out)
{
	bool prefer_up_only = namrbd_has_up_path(dev, tried_mask);
	u32 eligible = 0;
	u32 down = 0;
	u32 draining = 0;
	u32 i;

	if (dev->nr_paths <= 0)
		return NAMRBD_NO_PATH_PLAN_EMPTY;

	for (i = 0; i < (u32)dev->nr_paths; i++) {
		struct namrbd_path *p = &dev->paths[i];

		if (tried_mask & (1UL << i))
			continue;
		if (p->state == NAMRBD_PATH_DOWN)
			down++;
		if (p->state == NAMRBD_PATH_DRAINING)
			draining++;
		if (namrbd_path_eligible(op, p, prefer_up_only))
			eligible++;
	}
	if (eligible_out)
		*eligible_out = eligible;
	if (eligible > 0)
		return NAMRBD_NO_PATH_NONE;
	if (tried_mask)
		return NAMRBD_NO_PATH_EXHAUSTED_AFTER_RETRY;
	if (down == (u32)dev->nr_paths)
		return NAMRBD_NO_PATH_ALL_DOWN;
	if (draining == (u32)dev->nr_paths)
		return NAMRBD_NO_PATH_ALL_DRAINING;
	return NAMRBD_NO_PATH_NO_ELIGIBLE;
}

static blk_status_t namrbd_fail_no_path(struct namrbd_blk_dev *dev, namrbd_req_op_t op,
					ulong tried_mask,
					enum namrbd_no_path_reason reason)
{
	u32 eligible = 0;

	if (reason == NAMRBD_NO_PATH_NONE)
		reason = namrbd_classify_no_path(dev, op, tried_mask, &eligible);

	dev->last_no_path_reason = (u32)reason;
	dev->last_no_path_op = (u32)op;
	dev->last_no_path_eligible_paths = eligible;
	dev->last_no_path_tried_mask = (u64)tried_mask;
	dev->last_no_path_jiffies = jiffies;
	if (dev->no_path_state != NAMRBD_NO_PATH_FAILING)
		atomic64_inc(&dev->no_path_enter_count);
	dev->no_path_state = NAMRBD_NO_PATH_FAILING;
	if (!dev->no_path_since_jiffies)
		dev->no_path_since_jiffies = jiffies;
	if (dev->no_path_retry_mode == NAMRBD_NO_PATH_RETRY_TIMED)
		dev->no_path_retry_deadline_jiffies =
			dev->no_path_since_jiffies + dev->no_path_retry_seconds * HZ;
	else
		dev->no_path_retry_deadline_jiffies = 0;
	atomic64_inc(&dev->no_path_reqs);
	atomic64_inc(&dev->no_path_failed_reqs);
	NAMRBD_TRACE("device=%u no_path op=%u reason=%s eligible=%u tried=0x%llx\n",
		     dev->device_id, op, namrbd_no_path_reason_str(reason),
		     eligible, (unsigned long long)tried_mask);
	NAMRBD_TRACE_INFO("device=%u no_path_fail op=%u reason=%s eligible=%u tried=0x%llx retry_mode=%s failed_reqs_total=%lld queued_reqs_total=%lld recovered_reqs_total=%lld last_failed_path=%u last_errno=%lld\n",
			  dev->device_id, op, namrbd_no_path_reason_str(reason),
			  eligible, (unsigned long long)tried_mask,
			  namrbd_no_path_retry_mode_str(dev->no_path_retry_mode),
			  atomic64_read(&dev->no_path_failed_reqs),
			  atomic64_read(&dev->no_path_queued_reqs),
			  atomic64_read(&dev->no_path_recovered_reqs),
			  dev->last_failed_path_id,
			  atomic64_read(&dev->last_errno));
	return BLK_STS_IOERR;
}

static void namrbd_wake_no_path_queue(struct namrbd_blk_dev *dev, const char *reason)
{
	if (!dev || !dev->queue)
		return;
	if (dev->no_path_state != NAMRBD_NO_PATH_QUEUEING)
		return;
	dev->no_path_state = NAMRBD_NO_PATH_RECOVERING;
	dev->last_no_path_wakeup_jiffies = jiffies;
	NAMRBD_TRACE("device=%u no_path wake reason=%s\n",
		     dev->device_id, reason ? reason : "unknown");
	blk_mq_run_hw_queues(dev->queue, true);
}

static void namrbd_kick_no_path_queue(struct namrbd_blk_dev *dev, const char *reason)
{
	if (!dev || !dev->queue)
		return;
	if (dev->no_path_state != NAMRBD_NO_PATH_QUEUEING)
		return;
	dev->last_no_path_wakeup_jiffies = jiffies;
	NAMRBD_TRACE("device=%u no_path kick reason=%s\n",
		     dev->device_id, reason ? reason : "unknown");
	blk_mq_delay_kick_requeue_list(dev->queue, 1);
	blk_mq_run_hw_queues(dev->queue, true);
}

static bool namrbd_requeue_no_path(struct namrbd_blk_dev *dev, struct request *rq,
				   namrbd_req_op_t op, ulong tried_mask,
				   enum namrbd_no_path_reason reason)
{
	unsigned long delay_ms;
	u32 eligible = 0;

	if (reason == NAMRBD_NO_PATH_NONE)
		reason = namrbd_classify_no_path(dev, op, tried_mask, &eligible);

	if (reason == NAMRBD_NO_PATH_DETACHED ||
	    reason == NAMRBD_NO_PATH_PLAN_EMPTY ||
	    dev->no_path_retry_mode == NAMRBD_NO_PATH_RETRY_FAIL)
		return false;

	if (dev->no_path_retry_mode == NAMRBD_NO_PATH_RETRY_TIMED) {
		if (!dev->no_path_since_jiffies)
			dev->no_path_since_jiffies = jiffies;
		if (!dev->no_path_retry_deadline_jiffies)
			dev->no_path_retry_deadline_jiffies =
				dev->no_path_since_jiffies + dev->no_path_retry_seconds * HZ;
		if (time_after_eq(jiffies, dev->no_path_retry_deadline_jiffies))
			return false;
	} else {
		dev->no_path_retry_deadline_jiffies = 0;
	}

	dev->last_no_path_reason = (u32)reason;
	dev->last_no_path_op = (u32)op;
	dev->last_no_path_eligible_paths = eligible;
	dev->last_no_path_tried_mask = (u64)tried_mask;
	dev->last_no_path_jiffies = jiffies;
	if (dev->no_path_state != NAMRBD_NO_PATH_QUEUEING)
		atomic64_inc(&dev->no_path_enter_count);
	dev->no_path_state = NAMRBD_NO_PATH_QUEUEING;
	if (!dev->no_path_since_jiffies)
		dev->no_path_since_jiffies = jiffies;
	atomic64_inc(&dev->no_path_reqs);
	atomic64_inc(&dev->no_path_queued_reqs);
	atomic64_inc(&dev->no_path_requeued_reqs);

	delay_ms = no_path_requeue_delay_ms ? no_path_requeue_delay_ms : 1;
	NAMRBD_TRACE("device=%u no_path requeue op=%u reason=%s delay_ms=%u tried=0x%llx\n",
		     dev->device_id, op, namrbd_no_path_reason_str(reason),
		     no_path_requeue_delay_ms, (unsigned long long)tried_mask);
	NAMRBD_TRACE_INFO("device=%u no_path_requeue op=%u reason=%s eligible=%u tried=0x%llx retry_mode=%s delay_ms=%lu queued_reqs_total=%lld requeued_reqs_total=%lld last_failed_path=%u last_errno=%lld\n",
			  dev->device_id, op, namrbd_no_path_reason_str(reason),
			  eligible, (unsigned long long)tried_mask,
			  namrbd_no_path_retry_mode_str(dev->no_path_retry_mode),
			  delay_ms, atomic64_read(&dev->no_path_queued_reqs),
			  atomic64_read(&dev->no_path_requeued_reqs),
			  dev->last_failed_path_id,
			  atomic64_read(&dev->last_errno));
	blk_mq_requeue_request(rq, false);
	blk_mq_delay_kick_requeue_list(dev->queue, delay_ms);
	return true;
}

static bool namrbd_datapath_resource_busy(int err)
{
	return err == -EBUSY;
}

static void namrbd_kick_datapath_resource_requeue(struct namrbd_blk_dev *dev)
{
	if (!dev || !dev->queue)
		return;

	/* Resource requeues are completion-driven; the timer is only a backstop. */
	blk_mq_delay_kick_requeue_list(dev->queue, 0);
	blk_mq_run_hw_queues(dev->queue, true);
}

static bool namrbd_resource_requeue_is_tracked_locked(struct namrbd_blk_dev *dev,
						      struct request *rq)
{
	struct namrbd_resource_requeued_req *entry;

	list_for_each_entry(entry, &dev->data_resource_requeue_list, list) {
		if (entry->rq == rq)
			return true;
	}
	return false;
}

static bool namrbd_track_resource_requeue(struct namrbd_blk_dev *dev,
					  struct request *rq)
{
	struct namrbd_resource_requeued_req *entry;
	unsigned long flags;

	if (!dev || !rq)
		return false;

	entry = kzalloc(sizeof(*entry), GFP_ATOMIC);
	if (!entry)
		return false;
	entry->rq = rq;

	spin_lock_irqsave(&dev->data_resource_requeue_lock, flags);
	if (namrbd_resource_requeue_is_tracked_locked(dev, rq)) {
		spin_unlock_irqrestore(&dev->data_resource_requeue_lock, flags);
		kfree(entry);
		return true;
	}
	list_add_tail(&entry->list, &dev->data_resource_requeue_list);
	atomic_inc(&dev->data_resource_requeued_reqs);
	atomic64_inc(&dev->data_resource_requeue_events);
	spin_unlock_irqrestore(&dev->data_resource_requeue_lock, flags);
	return true;
}

static bool namrbd_untrack_resource_requeue(struct namrbd_blk_dev *dev,
					    struct request *rq)
{
	struct namrbd_resource_requeued_req *entry;
	struct namrbd_resource_requeued_req *tmp;
	unsigned long flags;

	if (!dev || !rq)
		return false;

	spin_lock_irqsave(&dev->data_resource_requeue_lock, flags);
	list_for_each_entry_safe(entry, tmp, &dev->data_resource_requeue_list, list) {
		if (entry->rq != rq)
			continue;
		list_del(&entry->list);
		if (atomic_read(&dev->data_resource_requeued_reqs) > 0)
			atomic_dec(&dev->data_resource_requeued_reqs);
		spin_unlock_irqrestore(&dev->data_resource_requeue_lock, flags);
		kfree(entry);
		return true;
	}
	spin_unlock_irqrestore(&dev->data_resource_requeue_lock, flags);
	return false;
}

static void namrbd_cleanup_resource_requeues(struct namrbd_blk_dev *dev)
{
	struct namrbd_resource_requeued_req *entry;
	struct namrbd_resource_requeued_req *tmp;
	LIST_HEAD(to_free);
	unsigned long flags;

	if (!dev)
		return;

	spin_lock_irqsave(&dev->data_resource_requeue_lock, flags);
	list_splice_init(&dev->data_resource_requeue_list, &to_free);
	atomic_set(&dev->data_resource_requeued_reqs, 0);
	spin_unlock_irqrestore(&dev->data_resource_requeue_lock, flags);

	list_for_each_entry_safe(entry, tmp, &to_free, list) {
		list_del(&entry->list);
		kfree(entry);
	}
}

static bool namrbd_requeue_datapath_resource(struct namrbd_blk_dev *dev,
					     struct request *rq,
					     namrbd_req_op_t op,
					     ulong tried_mask,
					     const char *reason)
{
	if (!dev || !dev->queue || !rq)
		return false;
	if (!namrbd_track_resource_requeue(dev, rq))
		return false;

	atomic64_inc(&dev->retry_reqs);
	NAMRBD_TRACE_INFO("device=%u datapath_resource_requeue op=%u reason=%s tried=0x%llx inflight_reqs=%d inflight_bytes=%lld requeued_reqs=%d max_inflight_reqs=%u max_inflight_bytes=%llu backstop_delay_ms=%u\n",
			  dev->device_id, op, reason ? reason : "busy",
			  (unsigned long long)tried_mask,
			  atomic_read(&dev->data_inflight_reqs),
			  atomic64_read(&dev->data_inflight_bytes),
			  atomic_read(&dev->data_resource_requeued_reqs),
			  dev->max_inflight_requests,
			  (unsigned long long)dev->max_inflight_bytes,
			  NAMRBD_RESOURCE_REQUEUE_BACKSTOP_MS);
	blk_mq_requeue_request(rq, false);
	blk_mq_delay_kick_requeue_list(dev->queue,
				       NAMRBD_RESOURCE_REQUEUE_BACKSTOP_MS);
	return true;
}

static bool namrbd_requeue_flush_until_datapath_quiesced(struct namrbd_blk_dev *dev,
							 struct request *rq,
							 ulong tried_mask)
{
	int inflight_reqs;
	int requeued_reqs;
	s64 inflight_bytes;

	if (!dev || !dev->queue || !rq)
		return false;

	inflight_reqs = atomic_read(&dev->data_inflight_reqs);
	inflight_bytes = atomic64_read(&dev->data_inflight_bytes);
	requeued_reqs = atomic_read(&dev->data_resource_requeued_reqs);
	if (inflight_reqs <= 0 && inflight_bytes <= 0 && requeued_reqs <= 0)
		return false;

	atomic64_inc(&dev->retry_reqs);
	NAMRBD_TRACE_INFO("device=%u flush_requeue reason=datapath-quiesce tried=0x%llx inflight_reqs=%d inflight_bytes=%lld requeued_reqs=%d backstop_delay_ms=%u\n",
			  dev->device_id, (unsigned long long)tried_mask,
			  inflight_reqs, inflight_bytes, requeued_reqs,
			  NAMRBD_RESOURCE_REQUEUE_BACKSTOP_MS);
	blk_mq_requeue_request(rq, false);
	blk_mq_delay_kick_requeue_list(dev->queue,
				       NAMRBD_RESOURCE_REQUEUE_BACKSTOP_MS);
	return true;
}

static u32 namrbd_request_lane_id(struct namrbd_blk_dev *dev, struct blk_mq_hw_ctx *hctx,
				  struct request *rq)
{
	namrbd_req_op_t op;
	u64 pos;
	u64 range_bytes;
	u64 range_index;
	u32 lane_id;

	if (!dev || dev->active_lane_count == 0)
		return NAMRBD_LANE_ID_NONE;
	if (dev->active_lane_count == 1)
		return 0;

	if (rq) {
		op = (namrbd_req_op_t)req_op(rq);
		if (op == REQ_OP_WRITE || op == REQ_OP_DISCARD ||
		    op == REQ_OP_WRITE_ZEROES) {
			range_bytes = dev->chunk_size_bytes ?
				dev->chunk_size_bytes : NAMRBD_BLOCK_SIZE;
			pos = (u64)blk_rq_pos(rq) << NAMRBD_SECTOR_SHIFT;
			range_index = div64_u64(pos, range_bytes);
			return (u32)(range_index % dev->active_lane_count);
		}
	}

	/*
	 * NBD-style dispatch: blk-mq hardware queue identity selects the
	 * gateway lane. Each lane owns a persistent path connection, so
	 * multi-queue workloads spread while each connection keeps send order.
	 */
	if (hctx)
		return hctx->queue_num % dev->active_lane_count;

	lane_id = dev->rr_cursor++ % dev->active_lane_count;
	return lane_id;
}

static struct namrbd_path *namrbd_pick_path_for_lane(struct namrbd_blk_dev *dev,
						     namrbd_req_op_t op,
						     u32 lane_id,
						     ulong tried_mask)
{
	int i;
	int best = -1;
	bool prefer_up_only = namrbd_has_up_path(dev, tried_mask);
	u32 limit = dev->nr_paths;
	u32 preferred_path_id = NAMRBD_PATH_ID_NONE;
	u32 fallback_path_id = NAMRBD_PATH_ID_NONE;

	if (lane_id != NAMRBD_LANE_ID_NONE && lane_id < dev->active_lane_count) {
		preferred_path_id = dev->lane_preferred_path_ids[lane_id];
		fallback_path_id = namrbd_lane_fallback_path_id(dev, preferred_path_id);
	}

	if (preferred_path_id != NAMRBD_PATH_ID_NONE) {
		int slot = namrbd_path_slot_by_id(dev, preferred_path_id);

		if (slot >= 0 && slot < limit &&
		    !(tried_mask & (1UL << slot)) &&
		    namrbd_path_eligible(op, &dev->paths[slot], prefer_up_only)) {
			dev->last_selected_lane_id = lane_id;
			dev->last_selected_path_id = dev->paths[slot].path_id;
			if (tried_mask)
				dev->last_failover_to_path_id = dev->paths[slot].path_id;
			return &dev->paths[slot];
		}
	}
	if (fallback_path_id != NAMRBD_PATH_ID_NONE) {
		int slot = namrbd_path_slot_by_id(dev, fallback_path_id);

		if (slot >= 0 && slot < limit &&
		    !(tried_mask & (1UL << slot)) &&
		    namrbd_path_eligible(op, &dev->paths[slot], prefer_up_only)) {
			dev->last_selected_lane_id = lane_id;
			dev->last_selected_path_id = dev->paths[slot].path_id;
			if (tried_mask)
				dev->last_failover_to_path_id = dev->paths[slot].path_id;
			return &dev->paths[slot];
		}
	}

	switch (dev->policy) {
	case NAMRBD_SCHED_RR:
		for (i = 0; i < limit; i++) {
			u32 idx = (dev->rr_cursor + i) % limit;
			struct namrbd_path *p = &dev->paths[idx];

			if (tried_mask & (1UL << idx))
				continue;
			if (!namrbd_path_eligible(op, p, prefer_up_only))
				continue;
			dev->rr_cursor = idx + 1;
			dev->last_selected_lane_id = lane_id;
			dev->last_selected_path_id = p->path_id;
			if (tried_mask)
				dev->last_failover_to_path_id = p->path_id;
			return p;
		}
		return NULL;
	case NAMRBD_SCHED_EWMA:
		for (i = 0; i < limit; i++) {
			struct namrbd_path *p = &dev->paths[i];
			u64 lat;

			if (tried_mask & (1UL << i))
				continue;
			if (!namrbd_path_eligible(op, p, prefer_up_only))
				continue;

			spin_lock(&p->lock);
			lat = p->ewma_latency_ns;
			spin_unlock(&p->lock);

			if (best < 0) {
				best = i;
				continue;
			}
			spin_lock(&dev->paths[best].lock);
			if (lat < dev->paths[best].ewma_latency_ns)
				best = i;
			spin_unlock(&dev->paths[best].lock);
		}
		break;
	case NAMRBD_SCHED_LEAST_INFLIGHT:
	default:
		for (i = 0; i < limit; i++) {
			struct namrbd_path *p = &dev->paths[i];
			int inflight;
			int best_inflight;

			if (tried_mask & (1UL << i))
				continue;
			if (!namrbd_path_eligible(op, p, prefer_up_only))
				continue;

			inflight = atomic_read(&p->inflight);
			if (best < 0) {
				best = i;
				continue;
			}
			best_inflight = atomic_read(&dev->paths[best].inflight);
			if (inflight < best_inflight)
				best = i;
		}
		break;
	}

	if (best < 0)
		return NULL;
	dev->last_selected_lane_id = lane_id;
	dev->last_selected_path_id = dev->paths[best].path_id;
	if (tried_mask)
		dev->last_failover_to_path_id = dev->paths[best].path_id;
	return &dev->paths[best];
}

static void namrbd_path_complete(struct namrbd_path *p, u64 latency_ns, bool retry)
{
	u64 old;
	u64 ewma;

	spin_lock(&p->lock);
	old = p->ewma_latency_ns;
	if (old == 0)
		ewma = latency_ns;
	else
		ewma = (old * 7 + latency_ns) / 8;
	p->ewma_latency_ns = ewma;
	p->completed++;
	if (retry)
		p->retries++;
	spin_unlock(&p->lock);
}

static u32 namrbd_max_io_sectors(u32 max_io_size)
{
	u32 sectors;

	if (!max_io_size)
		max_io_size = NAMRBD_DEFAULT_MAX_IO_SIZE;
	sectors = max_io_size / NAMRBD_SECTOR_SIZE;
	if (!sectors)
		sectors = 1;
	return sectors;
}

static u32 namrbd_effective_data_io_size(u32 zero_like_io_size)
{
	u32 data_io_size = data_max_io_size ? data_max_io_size :
			   NAMRBD_DEFAULT_MAX_DATA_IO_SIZE;

	if (!zero_like_io_size)
		zero_like_io_size = NAMRBD_DEFAULT_MAX_IO_SIZE;
	if (data_io_size > zero_like_io_size)
		data_io_size = zero_like_io_size;
	if (data_io_size < NAMRBD_SECTOR_SIZE)
		data_io_size = NAMRBD_SECTOR_SIZE;
	return data_io_size;
}

static u32 namrbd_max_io_size_for_op(struct namrbd_blk_dev *dev, namrbd_req_op_t op)
{
	if (!dev)
		return NAMRBD_DEFAULT_MAX_IO_SIZE;

	switch (op) {
	case REQ_OP_READ:
	case REQ_OP_WRITE:
		return dev->max_data_io_size ? dev->max_data_io_size :
					       NAMRBD_DEFAULT_MAX_DATA_IO_SIZE;
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		return dev->max_zero_like_io_size ? dev->max_zero_like_io_size :
						    (dev->max_io_size ? dev->max_io_size :
									      NAMRBD_DEFAULT_MAX_IO_SIZE);
	default:
		return dev->max_io_size ? dev->max_io_size :
					  NAMRBD_DEFAULT_MAX_IO_SIZE;
	}
}

static void namrbd_apply_discard_queue_limits(struct queue_limits *lim,
					      u32 zero_like_io_sectors)
{
	if (!lim)
		return;

	lim->discard_granularity = NAMRBD_BLOCK_SIZE;
	lim->discard_alignment = 0;
	lim->max_hw_discard_sectors = zero_like_io_sectors;
	lim->max_discard_sectors = zero_like_io_sectors;
	lim->max_write_zeroes_sectors = zero_like_io_sectors;
}

#if !NAMRBD_HAVE_QUEUE_LIMITS_COMMIT_UPDATE
static void namrbd_set_legacy_queue_limits(struct request_queue *queue,
					   u32 data_io_sectors,
					   u32 zero_like_io_sectors)
{
	if (!queue)
		return;

	queue->limits.logical_block_size = NAMRBD_BLOCK_SIZE;
	queue->limits.physical_block_size = NAMRBD_BLOCK_SIZE;
	queue->limits.io_min = NAMRBD_BLOCK_SIZE;
	queue->limits.io_opt = NAMRBD_BLOCK_SIZE;
	queue->limits.max_hw_sectors = data_io_sectors;
	queue->limits.max_sectors = data_io_sectors;
	namrbd_apply_discard_queue_limits(&queue->limits, zero_like_io_sectors);
}
#endif

static int namrbd_apply_queue_limits(struct namrbd_blk_dev *dev, u32 max_io_size,
				     u32 max_zero_like_io_size, const char *reason)
{
	u32 data_io_size = namrbd_effective_data_io_size(max_io_size);
	u32 data_io_sectors = namrbd_max_io_sectors(data_io_size);
	u32 zero_like_io_size = max_zero_like_io_size ? max_zero_like_io_size :
						 max_io_size;
	u32 zero_like_io_sectors = namrbd_max_io_sectors(zero_like_io_size);
	int ret = 0;

	if (!dev || !dev->queue)
		return 0;

#if NAMRBD_HAVE_QUEUE_LIMITS_COMMIT_UPDATE
	{
		struct queue_limits lim = queue_limits_start_update(dev->queue);

		lim.max_hw_sectors = data_io_sectors;
		lim.max_sectors = data_io_sectors;
		namrbd_apply_discard_queue_limits(&lim, zero_like_io_sectors);
		ret = queue_limits_commit_update(dev->queue, &lim);
	}
#else
	namrbd_set_legacy_queue_limits(dev->queue, data_io_sectors,
				       zero_like_io_sectors);
#endif
	if (ret) {
		pr_warn("namrbd_blk: device_id=%u queue limits update failed reason=%s max_io_size=%u max_data_io_size=%u max_zero_like_io_size=%u data_io_sectors=%u zero_like_io_sectors=%u err=%d\n",
			dev->device_id, reason ? reason : "unknown",
			max_io_size, data_io_size, zero_like_io_size, data_io_sectors,
			zero_like_io_sectors, ret);
		return ret;
	}
	dev->max_io_size = max_io_size ? max_io_size : NAMRBD_DEFAULT_MAX_IO_SIZE;
	dev->max_data_io_size = data_io_size;
	dev->max_zero_like_io_size = zero_like_io_size ? zero_like_io_size :
						      dev->max_io_size;

	pr_info("namrbd_blk: device_id=%u queue limits updated reason=%s max_io_size=%u max_data_io_size=%u max_zero_like_io_size=%u data_io_sectors=%u zero_like_io_sectors=%u\n",
		dev->device_id, reason ? reason : "unknown",
		dev->max_io_size, dev->max_data_io_size, dev->max_zero_like_io_size,
		data_io_sectors, zero_like_io_sectors);
	return 0;
}

static void namrbd_wire_encode_header(u8 *buf, u32 op, u32 flags, u64 request_id,
				      u64 volume_id, u64 generation, u64 offset_bytes,
				      u32 length_bytes)
{
	u32 crc;

	put_unaligned_le32(0x4E4D4252, &buf[0]);
	put_unaligned_le16(1, &buf[4]);
	put_unaligned_le16(NAMRBD_WIRE_HDR_LEN, &buf[6]);
	put_unaligned_le32(op, &buf[8]);
	put_unaligned_le32(flags, &buf[12]);
	put_unaligned_le64(request_id, &buf[16]);
	put_unaligned_le64(volume_id, &buf[24]);
	put_unaligned_le64(generation, &buf[32]);
	put_unaligned_le64(offset_bytes, &buf[40]);
	put_unaligned_le32(length_bytes, &buf[48]);
	put_unaligned_le32(0, &buf[52]);
	crc = crc32c(~0, buf, NAMRBD_WIRE_HDR_LEN - 4) ^ ~0;
	put_unaligned_le32(crc, &buf[52]);
}

static u32 namrbd_wire_response_op(u32 op)
{
	switch (op) {
	case NAMRBD_WIRE_OP_READ:
		return NAMRBD_WIRE_OP_READ_RESP;
	case NAMRBD_WIRE_OP_WRITE:
		return NAMRBD_WIRE_OP_WRITE_RESP;
	case NAMRBD_WIRE_OP_FLUSH:
		return NAMRBD_WIRE_OP_FLUSH_RESP;
	case NAMRBD_WIRE_OP_DISCARD:
		return NAMRBD_WIRE_OP_DISCARD_RESP;
	case NAMRBD_WIRE_OP_WRITE_ZEROES:
		return NAMRBD_WIRE_OP_WRITE_ZEROES_RESP;
	case NAMRBD_WIRE_OP_PATH_PROBE:
		return NAMRBD_WIRE_OP_PATH_PROBE;
	default:
		return NAMRBD_WIRE_OP_ERROR_RESP;
	}
}

static u32 namrbd_wire_op_for_req(namrbd_req_op_t op)
{
	switch (op) {
	case REQ_OP_READ:
		return NAMRBD_WIRE_OP_READ;
	case REQ_OP_WRITE:
		return NAMRBD_WIRE_OP_WRITE;
	case REQ_OP_DISCARD:
		return NAMRBD_WIRE_OP_DISCARD;
	case REQ_OP_WRITE_ZEROES:
		return NAMRBD_WIRE_OP_WRITE_ZEROES;
	default:
		return 0;
	}
}

static u32 namrbd_wire_request_length(struct request *rq, namrbd_req_op_t op,
				      u32 payload_len)
{
	if (op == REQ_OP_WRITE)
		return payload_len;
	return blk_rq_bytes(rq);
}

static int namrbd_wire_validate_response_header(const u8 *hdr, u32 expected_op,
						u64 request_id, u64 volume_id,
						u64 generation)
{
	u32 resp_op;

	if (get_unaligned_le32(&hdr[0]) != NAMRBD_WIRE_MAGIC)
		return -EBADMSG;
	if (get_unaligned_le16(&hdr[4]) != NAMRBD_WIRE_VERSION)
		return -EBADMSG;
	if (get_unaligned_le16(&hdr[6]) != NAMRBD_WIRE_RESP_LEN)
		return -EBADMSG;

	resp_op = get_unaligned_le32(&hdr[8]);
	if (resp_op != expected_op && resp_op != NAMRBD_WIRE_OP_ERROR_RESP)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[16]) != request_id)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[24]) != volume_id)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[32]) != generation)
		return -ESTALE;
	return 0;
}

/* ----- Wire v2 (Phase C3) helpers ----- */
static const char namrbd_b64url[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/* Encode 16 bytes to base64url (22 chars, no padding) */
static void namrbd_b64url_encode_16(const u8 *in, char *out)
{
	u32 v;
	int i;

	for (i = 0; i < 15; i += 3) {
		v = in[i] << 16 | in[i + 1] << 8 | in[i + 2];
		*out++ = namrbd_b64url[(v >> 18) & 0x3f];
		*out++ = namrbd_b64url[(v >> 12) & 0x3f];
		*out++ = namrbd_b64url[(v >> 6) & 0x3f];
		*out++ = namrbd_b64url[v & 0x3f];
	}
	v = in[15];
	*out++ = namrbd_b64url[(v >> 2) & 0x3f];
	*out++ = namrbd_b64url[(v << 4) & 0x3f];
	*out = '\0';
}

/* Derive session key: HMAC-SHA256(key=session_key_or_token, data=token||\0||client_nonce||\0||server_nonce||\0||session_id_le64) */
static int namrbd_wirev2_derive_session_key(const char *key, size_t key_len,
					    const char *token, size_t token_len,
					    const char *client_nonce, const char *server_nonce,
					    u64 session_id, u8 *key_out)
{
	struct crypto_shash *tfm;
	struct shash_desc *desc;
	size_t desc_size;
	u8 *buf = NULL;
	size_t buf_len;
	int ret;

	tfm = crypto_alloc_shash("hmac(sha256)", 0, 0);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);
	desc_size = crypto_shash_descsize(tfm) + sizeof(*desc);
	desc = kzalloc(desc_size, GFP_KERNEL);
	if (!desc) {
		ret = -ENOMEM;
		goto out;
	}
	buf_len = token_len + 1 + strlen(client_nonce) + 1 + strlen(server_nonce) + 1 + 8;
	buf = kzalloc(buf_len, GFP_KERNEL);
	if (!buf) {
		ret = -ENOMEM;
		goto out;
	}
	memcpy(buf, token, token_len);
	buf[token_len] = 0;
	memcpy(buf + token_len + 1, client_nonce, strlen(client_nonce));
	buf[token_len + 1 + strlen(client_nonce)] = 0;
	memcpy(buf + token_len + 1 + strlen(client_nonce) + 1, server_nonce, strlen(server_nonce));
	buf[token_len + 1 + strlen(client_nonce) + 1 + strlen(server_nonce)] = 0;
	put_unaligned_le64(session_id, buf + buf_len - 8);

	desc->tfm = tfm;
	ret = crypto_shash_setkey(tfm, (const u8 *)key, key_len);
	if (ret)
		goto out;
	ret = crypto_shash_init(desc);
	if (ret)
		goto out;
	ret = crypto_shash_update(desc, buf, buf_len);
	if (ret)
		goto out;
	ret = crypto_shash_final(desc, key_out);
out:
	kfree(buf);
	kfree(desc);
	crypto_free_shash(tfm);
	return ret;
}

/* AAD 58 bytes: version(2), op(4), request_id(8), volume_id(8), generation(8), session_id(8), seq_no(8), offset(8), length(4) */
static void namrbd_wirev2_build_aad(const u8 *hdr, u8 *aad)
{
	put_unaligned_le16(NAMRBD_WIREV2_VERSION, &aad[0]);
	put_unaligned_le32(get_unaligned_le32(&hdr[8]), &aad[2]);
	put_unaligned_le64(get_unaligned_le64(&hdr[16]), &aad[6]);
	put_unaligned_le64(get_unaligned_le64(&hdr[24]), &aad[14]);
	put_unaligned_le64(get_unaligned_le64(&hdr[32]), &aad[22]);
	put_unaligned_le64(get_unaligned_le64(&hdr[40]), &aad[30]);
	put_unaligned_le64(get_unaligned_le64(&hdr[48]), &aad[38]);
	put_unaligned_le64(get_unaligned_le64(&hdr[56]), &aad[46]);
	put_unaligned_le32(get_unaligned_le32(&hdr[64]), &aad[54]);
}

static int namrbd_wirev2_compute_auth_tag(const u8 *session_key, const u8 *hdr,
					  const u8 *payload, u32 payload_len, u8 *tag_out)
{
	struct crypto_shash *tfm;
	struct shash_desc *desc;
	size_t desc_size;
	u8 aad[58];
	int ret;

	namrbd_wirev2_build_aad(hdr, aad);
	tfm = crypto_alloc_shash("hmac(sha256)", 0, 0);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);
	desc_size = crypto_shash_descsize(tfm) + sizeof(*desc);
	desc = kzalloc(desc_size, GFP_KERNEL);
	if (!desc) {
		ret = -ENOMEM;
		goto out;
	}
	desc->tfm = tfm;
	ret = crypto_shash_setkey(tfm, session_key, NAMRBD_WIREV2_AUTH_TAG_LEN);
	if (ret)
		goto out;
	ret = crypto_shash_init(desc);
	if (ret)
		goto out;
	ret = crypto_shash_update(desc, aad, sizeof(aad));
	if (ret)
		goto out;
	if (payload_len)
		ret = crypto_shash_update(desc, payload, payload_len);
	if (ret)
		goto out;
	ret = crypto_shash_final(desc, tag_out);
out:
	kfree(desc);
	crypto_free_shash(tfm);
	return ret;
}

static void namrbd_wirev2_encode_header(u8 *buf, u32 op, u32 flags, u64 request_id,
					u64 volume_id, u64 generation, u64 session_id, u64 seq_no,
					u64 offset_bytes, u32 length_bytes, u16 auth_len)
{
	u32 crc;

	put_unaligned_le32(NAMRBD_WIREV2_MAGIC, &buf[0]);
	put_unaligned_le16(NAMRBD_WIREV2_VERSION, &buf[4]);
	put_unaligned_le16(NAMRBD_WIREV2_HDR_LEN, &buf[6]);
	put_unaligned_le32(op, &buf[8]);
	put_unaligned_le32(flags, &buf[12]);
	put_unaligned_le64(request_id, &buf[16]);
	put_unaligned_le64(volume_id, &buf[24]);
	put_unaligned_le64(generation, &buf[32]);
	put_unaligned_le64(session_id, &buf[40]);
	put_unaligned_le64(seq_no, &buf[48]);
	put_unaligned_le64(offset_bytes, &buf[56]);
	put_unaligned_le32(length_bytes, &buf[64]);
	put_unaligned_le16(auth_len, &buf[68]);
	put_unaligned_le16(0, &buf[70]);
	put_unaligned_le32(0, &buf[72]);
	crc = crc32c(~0, buf, 72) ^ ~0;
	put_unaligned_le32(crc, &buf[72]);
}

static int namrbd_wirev2_validate_response_header(const u8 *hdr, u32 expected_op,
						  u64 request_id, u64 volume_id,
						  u64 generation, u64 session_id,
						  u64 seq_no)
{
	u32 resp_op;

	if (get_unaligned_le32(&hdr[0]) != NAMRBD_WIREV2_MAGIC)
		return -EBADMSG;
	if (get_unaligned_le16(&hdr[4]) != NAMRBD_WIREV2_VERSION)
		return -EBADMSG;
	if (get_unaligned_le16(&hdr[6]) != NAMRBD_WIREV2_RESP_LEN)
		return -EBADMSG;

	resp_op = get_unaligned_le32(&hdr[8]);
	if (resp_op != expected_op && resp_op != NAMRBD_WIRE_OP_ERROR_RESP)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[16]) != request_id)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[24]) != volume_id)
		return -EBADMSG;
	if (get_unaligned_le64(&hdr[32]) != generation)
		return -ESTALE;
	if (session_id && get_unaligned_le64(&hdr[40]) != session_id)
		return -EBADMSG;
	if (seq_no && get_unaligned_le64(&hdr[48]) != seq_no)
		return -EBADMSG;
	return 0;
}

/* Parse HELLO_ACK JSON for session_id and server_nonce (minimal: find key and value) */
static int namrbd_wirev2_hello_ack_parse(const char *json, size_t json_len,
					 u64 *session_id_out, char *server_nonce_out, size_t nonce_len)
{
	const char *p;
	char *end;

	*session_id_out = 0;
	server_nonce_out[0] = '\0';
	p = strstr(json, "\"session_id\"");
	if (!p || p >= json + json_len)
		return -EBADMSG;
	p = strchr(p, ':');
	if (!p)
		return -EBADMSG;
	p++;
	*session_id_out = simple_strtoull(p, &end, 10);
	p = strstr(json, "\"server_nonce\"");
	if (!p || p >= json + json_len)
		return -EBADMSG;
	p = strchr(p, '"');
	if (!p)
		return -EBADMSG;
	p = strchr(p + 1, '"');
	if (!p)
		return -EBADMSG;
	p++;
	end = strchr(p, '"');
	if (!end || (size_t)(end - p) >= nonce_len)
		return -EBADMSG;
	memcpy(server_nonce_out, p, end - p);
	server_nonce_out[end - p] = '\0';
	return 0;
}

static int namrbd_rw_collect_payload(struct request *rq, namrbd_req_op_t op,
				     u8 **payload_out, u32 *payload_len_out)
{
	struct req_iterator iter;
	struct bio_vec bvec;
	u8 *payload;
	u32 bytes = blk_rq_bytes(rq);
	u32 copied = 0;

	if (op != REQ_OP_WRITE) {
		*payload_out = NULL;
		*payload_len_out = 0;
		return 0;
	}

	payload = kmalloc(bytes + 24, GFP_KERNEL);
	if (!payload)
		return -ENOMEM;
	memset(payload, 0, 24);
	rq_for_each_segment(bvec, rq, iter) {
		void *kaddr;

		kaddr = kmap_local_page(bvec.bv_page);
		if (!kaddr) {
			kfree(payload);
			return -EIO;
		}
		memcpy(payload + 24 + copied, (u8 *)kaddr + bvec.bv_offset, bvec.bv_len);
		kunmap_local(kaddr);
		copied += bvec.bv_len;
	}
	*payload_out = payload;
	*payload_len_out = bytes + 24;
	return 0;
}

static int namrbd_rw_copy_read_payload(struct request *rq, const u8 *payload, u32 payload_len)
{
	struct req_iterator iter;
	struct bio_vec bvec;
	u32 copied = 0;

	if (payload_len != blk_rq_bytes(rq))
		return -EINVAL;

	rq_for_each_segment(bvec, rq, iter) {
		void *kaddr;

		kaddr = kmap_local_page(bvec.bv_page);
		if (!kaddr)
			return -EIO;
		memcpy((u8 *)kaddr + bvec.bv_offset, payload + copied, bvec.bv_len);
		kunmap_local(kaddr);
		copied += bvec.bv_len;
	}
	return 0;
}

static int namrbd_status_to_errno(s32 status)
{
	switch (status) {
	case 0:
		return 0;
	case 7:
	case 10:
	case 11:
	case 12:
		return -EAGAIN;
	case 4:
		return -ENOENT;
	case 5:
		return -ESTALE;
	case 6:
		return -EINVAL;
	default:
		return -EIO;
	}
}

static void namrbd_pending_complete(struct namrbd_pending_req *pending,
				    int err, u32 wire_status)
{
	if (pending->completed)
		return;
	pending->err = err;
	pending->wire_status = wire_status;
	pending->completed = true;
	complete(&pending->done);
}

static void namrbd_path_add_pending(struct namrbd_path *path,
				    struct namrbd_pending_req *pending)
{
	unsigned long flags;
	u32 count = 0;
	struct namrbd_pending_req *iter;

	spin_lock_irqsave(&path->pending_lock, flags);
	list_add_tail(&pending->list, &path->pending_reqs);
	list_for_each_entry(iter, &path->pending_reqs, list) {
		if (!iter->completed)
			count++;
	}
	if (count > path->pending_high_water)
		path->pending_high_water = count;
	spin_unlock_irqrestore(&path->pending_lock, flags);

	spin_lock_irqsave(&path->lock, flags);
	path->submitted++;
	spin_unlock_irqrestore(&path->lock, flags);
}

static void namrbd_path_remove_pending(struct namrbd_path *path,
				       struct namrbd_pending_req *pending)
{
	unsigned long flags;

	spin_lock_irqsave(&path->pending_lock, flags);
	if (!list_empty(&pending->list))
		list_del_init(&pending->list);
	spin_unlock_irqrestore(&path->pending_lock, flags);
}

static struct namrbd_pending_req *
namrbd_path_find_pending_locked(struct namrbd_path *path, u64 request_id)
{
	struct namrbd_pending_req *pending;

	list_for_each_entry(pending, &path->pending_reqs, list) {
		if (pending->request_id == request_id && !pending->completed)
			return pending;
	}
	return NULL;
}

static void namrbd_path_complete_pending_by_id(struct namrbd_path *path, u64 request_id,
					       int err, u32 wire_status)
{
	struct namrbd_pending_req *pending;
	unsigned long flags;

	spin_lock_irqsave(&path->pending_lock, flags);
	pending = namrbd_path_find_pending_locked(path, request_id);
	if (pending)
		namrbd_pending_complete(pending, err, wire_status);
	spin_unlock_irqrestore(&path->pending_lock, flags);
}

static void namrbd_path_fail_pending(struct namrbd_path *path, int err)
{
	struct namrbd_pending_req *pending;
	struct namrbd_pending_req *tmp;
	LIST_HEAD(async_failed);
	unsigned long flags;

	spin_lock_irqsave(&path->pending_lock, flags);
	list_for_each_entry_safe(pending, tmp, &path->pending_reqs, list) {
		if (pending->processing || pending->completed)
			continue;
		if (pending->async) {
			pending->completed = true;
			list_del_init(&pending->list);
			list_add_tail(&pending->list, &async_failed);
		} else {
			namrbd_pending_complete(pending, err, 0);
		}
	}
	spin_unlock_irqrestore(&path->pending_lock, flags);

	list_for_each_entry_safe(pending, tmp, &async_failed, list) {
		list_del_init(&pending->list);
		namrbd_finish_async_pending(pending, err, 0, true);
	}
}

static u32 namrbd_path_pending_count(struct namrbd_path *path)
{
	struct namrbd_pending_req *pending;
	unsigned long flags;
	u32 count = 0;

	spin_lock_irqsave(&path->pending_lock, flags);
	list_for_each_entry(pending, &path->pending_reqs, list) {
		if (!pending->completed)
			count++;
	}
	spin_unlock_irqrestore(&path->pending_lock, flags);
	return count;
}

static bool namrbd_retry_async_pending(struct namrbd_pending_req *pending,
				       int err, u32 wire_status, u64 old_lat_ns)
{
	struct namrbd_blk_dev *dev = pending->dev;
	struct namrbd_path *old_path = pending->path;
	struct namrbd_path *path;
	namrbd_req_op_t op = pending->op;
	ulong tried_mask = pending->tried_mask;
	u32 attempt = pending->attempt + 1;
	int old_slot;
	bool resource_busy = false;

	if (!dev || !pending->rq || !dev->attached || dev->nr_paths <= 1)
		return false;
	if (!namrbd_op_is_dataplane(op))
		return false;

	old_slot = old_path ? (int)(old_path - dev->paths) : -1;
	if (old_slot >= 0 && old_slot < dev->nr_paths)
		tried_mask |= (1UL << old_slot);

	while (attempt < (u32)dev->nr_paths) {
		u64 start_ns;
		u64 lat_ns;
		int req_err;
		int slot;

		path = namrbd_pick_path_for_lane(dev, op, pending->lane_id, tried_mask);
		if (!path)
			break;

		slot = (int)(path - dev->paths);
		if (slot < 0 || slot >= dev->nr_paths)
			break;
		tried_mask |= (1UL << slot);

		atomic_inc(&path->inflight);
		if (pending->lane_id != NAMRBD_LANE_ID_NONE &&
		    pending->lane_id < NAMRBD_MAX_PATHS)
			atomic64_inc(&dev->lane_dispatch_reqs[pending->lane_id]);

		start_ns = ktime_get_ns();
		req_err = namrbd_data_path_submit_async(dev, pending->rq, path,
							pending->lane_id, true,
							tried_mask, attempt);
		if (!req_err) {
			namrbd_untrack_resource_requeue(dev, pending->rq);
			if (old_path)
				dev->last_failover_from_path_id = old_path->path_id;
			dev->last_failover_to_path_id = path->path_id;
			atomic64_inc(&dev->path_failover_reqs);
			atomic64_inc(&dev->retry_reqs);
			NAMRBD_TRACE_INFO("device=%u async_datapath_retry_queued op=%u sector=%llu bytes=%u lane=%u from_path=%u to_path=%u original_err=%d errno=%d wire_status=%u(%s) old_latency_ns=%llu tried=0x%llx attempt=%u\n",
					  dev->device_id, op,
					  (unsigned long long)blk_rq_pos(pending->rq),
					  blk_rq_bytes(pending->rq), pending->lane_id,
					  old_path ? old_path->path_id : NAMRBD_PATH_ID_NONE,
					  path->path_id, err, -err, wire_status,
					  namrbd_wire_status_str(wire_status),
					  (unsigned long long)old_lat_ns,
					  (unsigned long long)tried_mask, attempt);
			return true;
		}

		lat_ns = ktime_get_ns() - start_ns;
		atomic_dec(&path->inflight);
		if (namrbd_datapath_resource_busy(req_err)) {
			resource_busy = true;
			NAMRBD_TRACE_INFO("device=%u async_datapath_retry_resource_busy op=%u sector=%llu bytes=%u lane=%u path=%u req_err=%d errno=%d tried=0x%llx latency_ns=%llu attempt=%u inflight=%d\n",
					  dev->device_id, op,
					  (unsigned long long)blk_rq_pos(pending->rq),
					  blk_rq_bytes(pending->rq), pending->lane_id,
					  path->path_id, req_err, -req_err,
					  (unsigned long long)tried_mask,
					  (unsigned long long)lat_ns, attempt,
					  atomic_read(&path->inflight));
			break;
		}
		namrbd_path_complete(path, lat_ns, true);
		dev->last_failed_path_id = path->path_id;
		if (req_err == -EAGAIN || req_err == -ETIMEDOUT)
			atomic64_inc(&dev->timeout_reqs);
		namrbd_path_mark_failure(dev, path, (u32)-req_err, 0);
		atomic64_inc(&dev->retry_reqs);
		NAMRBD_TRACE_INFO("device=%u async_datapath_retry_submit_failed op=%u sector=%llu bytes=%u lane=%u path=%u req_err=%d errno=%d tried=0x%llx latency_ns=%llu attempt=%u\n",
				  dev->device_id, op,
				  (unsigned long long)blk_rq_pos(pending->rq),
				  blk_rq_bytes(pending->rq), pending->lane_id,
				  path->path_id, req_err, -req_err,
				  (unsigned long long)tried_mask,
				  (unsigned long long)lat_ns, attempt);
		attempt++;
	}

	if (resource_busy)
		return namrbd_requeue_datapath_resource(dev, pending->rq, op,
							tried_mask,
							"async-retry-busy");

	return false;
}

static void namrbd_finish_async_pending(struct namrbd_pending_req *pending,
					int err, u32 wire_status,
					bool already_removed)
{
	struct namrbd_blk_dev *dev;
	struct namrbd_path *path;
	blk_status_t st;
	u64 lat_ns;
	bool released_resource;

	if (!pending || !pending->async)
		return;

	dev = pending->dev;
	path = pending->path;
	if (!dev || !path) {
		kfree(pending);
		return;
	}

	if (!already_removed)
		namrbd_path_remove_pending(path, pending);
	else
		INIT_LIST_HEAD(&pending->list);

	released_resource = pending->sem_acquired || pending->accounted;
	if (pending->sem_acquired)
		up(&path->outstanding_sem);
	if (pending->accounted) {
		atomic_dec(&dev->data_inflight_reqs);
		atomic64_sub(pending->accounted_bytes, &dev->data_inflight_bytes);
	}
	atomic_dec(&path->inflight);
	lat_ns = ktime_get_ns() - pending->start_ns;
	namrbd_path_complete(path, lat_ns, pending->retry);

	st = err ? BLK_STS_IOERR : BLK_STS_OK;
	if (st == BLK_STS_OK) {
		namrbd_untrack_resource_requeue(dev, pending->rq);
		namrbd_zero_map_update_after_success(dev, pending->rq, pending->op);
		dev->last_completed_path_id = path->path_id;
		if (dev->no_path_state != NAMRBD_NO_PATH_INACTIVE) {
			dev->no_path_state = NAMRBD_NO_PATH_INACTIVE;
			dev->no_path_since_jiffies = 0;
			dev->no_path_retry_deadline_jiffies = 0;
			atomic64_inc(&dev->no_path_recovered_reqs);
		}
		namrbd_path_mark_success(dev, path);
		atomic64_inc(&dev->completed_reqs);
	} else {
		if (namrbd_datapath_resource_busy(err)) {
			NAMRBD_TRACE_INFO("device=%u async_datapath_resource_busy op=%u sector=%llu bytes=%u lane=%u path=%u req_err=%d errno=%d wire_status=%u(%s) latency_ns=%llu inflight=%d\n",
					  dev->device_id, pending->op,
					  (unsigned long long)blk_rq_pos(pending->rq),
					  blk_rq_bytes(pending->rq), pending->lane_id,
					  path->path_id, err, -err, wire_status,
					  namrbd_wire_status_str(wire_status),
					  (unsigned long long)lat_ns,
					  atomic_read(&path->inflight));
			if (namrbd_requeue_datapath_resource(dev, pending->rq,
							     pending->op,
							     pending->tried_mask,
							     "async-complete-busy")) {
				if (released_resource)
					namrbd_kick_datapath_resource_requeue(dev);
				kfree(pending);
				return;
			}
			atomic64_inc(&dev->failed_reqs);
			NAMRBD_TRACE_INFO("device=%u async_datapath_resource_requeue_failed op=%u sector=%llu bytes=%u lane=%u path=%u req_err=%d errno=%d wire_status=%u(%s) latency_ns=%llu inflight=%d\n",
					  dev->device_id, pending->op,
					  (unsigned long long)blk_rq_pos(pending->rq),
					  blk_rq_bytes(pending->rq), pending->lane_id,
					  path->path_id, err, -err, wire_status,
					  namrbd_wire_status_str(wire_status),
					  (unsigned long long)lat_ns,
					  atomic_read(&path->inflight));
			goto out_complete;
		}
		dev->last_failed_path_id = path->path_id;
		if (err == -EAGAIN || err == -ETIMEDOUT)
			atomic64_inc(&dev->timeout_reqs);
		namrbd_path_mark_failure(dev, path, (u32)-err, wire_status);
		if (namrbd_retry_async_pending(pending, err, wire_status, lat_ns)) {
			if (released_resource)
				namrbd_kick_datapath_resource_requeue(dev);
			kfree(pending);
			return;
		}
		if (namrbd_classify_no_path(dev, pending->op, pending->tried_mask, NULL) !=
		    NAMRBD_NO_PATH_NONE &&
		    namrbd_requeue_no_path(dev, pending->rq, pending->op,
					   pending->tried_mask,
					   NAMRBD_NO_PATH_NONE)) {
			if (released_resource)
				namrbd_kick_datapath_resource_requeue(dev);
			kfree(pending);
			return;
		}
		atomic64_inc(&dev->failed_reqs);
		NAMRBD_TRACE_INFO("device=%u async_datapath_failure op=%u sector=%llu bytes=%u lane=%u path=%u req_err=%d errno=%d wire_status=%u(%s) latency_ns=%llu inflight=%d\n",
				  dev->device_id, pending->op,
				  (unsigned long long)blk_rq_pos(pending->rq),
				  blk_rq_bytes(pending->rq), pending->lane_id,
				  path->path_id, err, -err, wire_status,
				  namrbd_wire_status_str(wire_status),
				  (unsigned long long)lat_ns,
				  atomic_read(&path->inflight));
		}

out_complete:
	NAMRBD_TRACE("device=%u async_complete op=%u status=%u path=%u lane=%u\n",
		     dev->device_id, pending->op, st, path->path_id, pending->lane_id);
	namrbd_untrack_resource_requeue(dev, pending->rq);
	blk_mq_end_request(pending->rq, st);
	if (released_resource)
		namrbd_kick_datapath_resource_requeue(dev);
	kfree(pending);
}

static void namrbd_path_detach_worker_socket(struct namrbd_path *path, struct socket *sock,
					     int err)
{
	struct task_struct *task = NULL;
	bool detached = false;

	mutex_lock(&path->io_lock);
	if (READ_ONCE(path->sock) == sock) {
		WRITE_ONCE(path->sock, NULL);
		task = path->recv_task;
		path->recv_task = NULL;
		path->connection_resets++;
		detached = true;
	}
	mutex_unlock(&path->io_lock);

	if (detached) {
		namrbd_path_fail_pending(path, err);
		sock_release(sock);
	}
	if (task)
		put_task_struct(task);
}

static int namrbd_path_recv_worker(void *arg)
{
	struct namrbd_path *path = arg;
	struct socket *sock = READ_ONCE(path->sock);
	u8 resp_hdr[NAMRBD_WIRE_RESP_LEN];
	int ret = 0;

	if (!sock)
		return 0;

	while (!kthread_should_stop()) {
		struct namrbd_pending_req *pending;
		u8 *resp_payload = NULL;
		u64 request_id;
		u32 resp_len;
		u32 wire_status;
		int req_err;

		ret = namrbd_transport_recv_all(sock, resp_hdr, sizeof(resp_hdr));
		if (ret)
			break;

		request_id = get_unaligned_le64(&resp_hdr[16]);
		spin_lock_irq(&path->pending_lock);
		pending = namrbd_path_find_pending_locked(path, request_id);
		if (pending)
			pending->processing = true;
		spin_unlock_irq(&path->pending_lock);
		if (!pending) {
			ret = -EBADMSG;
			break;
		}

		ret = namrbd_wire_validate_response_header(resp_hdr, pending->expected_op,
							   pending->request_id,
							   pending->volume_id,
							   pending->generation);
		if (ret)
			goto complete_one;

		resp_len = get_unaligned_le32(&resp_hdr[48]);
		if (resp_len > pending->max_resp_len) {
			ret = -EMSGSIZE;
			goto complete_one;
		}
		if (resp_len) {
			resp_payload = kmalloc(resp_len, GFP_KERNEL);
			if (!resp_payload) {
				ret = -ENOMEM;
				goto complete_one;
			}
			ret = namrbd_transport_recv_all(sock, resp_payload, resp_len);
			if (ret) {
				if (pending->async)
					namrbd_finish_async_pending(pending, ret, 0, false);
				else
					namrbd_path_complete_pending_by_id(path, request_id, ret, 0);
				kfree(resp_payload);
				break;
			}
		}

		wire_status = get_unaligned_le32(&resp_hdr[56]);
		req_err = namrbd_status_to_errno((s32)wire_status);
		if (!req_err && pending->op == REQ_OP_READ)
			req_err = namrbd_rw_copy_read_payload(pending->rq, resp_payload, resp_len);
		if (pending->async)
			namrbd_finish_async_pending(pending, req_err, wire_status, false);
		else
			namrbd_path_complete_pending_by_id(path, request_id, req_err, wire_status);
		kfree(resp_payload);
		continue;

complete_one:
		if (pending->async)
			namrbd_finish_async_pending(pending, ret, 0, false);
		else
			namrbd_path_complete_pending_by_id(path, request_id, ret, 0);
		kfree(resp_payload);
		break;
	}

	if (ret && !kthread_should_stop())
		namrbd_path_detach_worker_socket(path, sock, ret);
	return 0;
}

/* Wire v2 data path: HELLO -> HELLO_ACK -> one authenticated READ/WRITE */
static int namrbd_data_path_request_v2(struct namrbd_blk_dev *dev, struct request *rq,
					struct namrbd_path *path, u8 *payload, u32 payload_len,
					namrbd_req_op_t op, u32 *wire_status_out)
{
	struct socket *sock = NULL;
	u8 *hello_buf = NULL;
	size_t hello_len;
	u8 *hello_ack_buf = NULL;
	size_t hello_ack_len;
	u64 session_id = 0;
	char client_nonce[32];
	char server_nonce[128];
	u8 session_key[NAMRBD_WIREV2_AUTH_TAG_LEN];
	u8 *req_buf = NULL;
	size_t req_len;
	u8 *resp_buf = NULL;
	size_t resp_len;
	u64 request_id;
	u32 wire_op = (op == REQ_OP_READ) ? NAMRBD_WIREV2_OP_READ : NAMRBD_WIREV2_OP_WRITE;
	u32 expected_resp_op = namrbd_wire_response_op(wire_op);
	u64 offset_bytes = (u64)blk_rq_pos(rq) << 9;
	u32 length_bytes = (op == REQ_OP_WRITE) ? payload_len : blk_rq_bytes(rq);
	u32 max_resp_payload_len;
	int ret;
	size_t token_len = strlen(dev->dataplane_token);
	size_t session_key_len = strlen(dev->dataplane_session_key);
	const char *derivation_key = session_key_len ? dev->dataplane_session_key : dev->dataplane_token;
	size_t derivation_key_len = session_key_len ? session_key_len : token_len;
	u32 resp_payload_len;
	u16 resp_auth_len;

	if (op != REQ_OP_READ && op != REQ_OP_WRITE)
		return -EOPNOTSUPP;
	if (!token_len || token_len >= sizeof(dev->dataplane_token))
		return -EINVAL;

	request_id = atomic64_inc_return(&dev->request_seq);
	{
		u8 rnd[16];

		get_random_bytes(rnd, 16);
		namrbd_b64url_encode_16(rnd, client_nonce);
	}
	hello_len = NAMRBD_WIREV2_HDR_LEN + 256 + NAMRBD_MAX_HOST_ID_LEN + token_len;
	hello_buf = kzalloc(hello_len, GFP_KERNEL);
	if (!hello_buf)
		return -ENOMEM;
	{
		int n;
		u8 *hdr = hello_buf;
		char *body = (char *)(hello_buf + NAMRBD_WIREV2_HDR_LEN);

		namrbd_wirev2_encode_header(hdr, NAMRBD_WIREV2_OP_HELLO, 0, request_id,
					   dev->volume_id, dev->generation, 0, 0,
					   0, 0, 0);
		n = snprintf(body, hello_len - NAMRBD_WIREV2_HDR_LEN,
			     "{\"token\":\"%s\",\"client_nonce\":\"%s\",\"device_id\":%u,\"host_id\":\"%s\",\"supported_auth\":[\"token-hmac-v1\"],\"requested_path_id\":%u}",
			     dev->dataplane_token, client_nonce, dev->device_id,
			     dev->attached_host_id, path ? path->path_id : 0);
		if (n < 0 || (size_t)n >= hello_len - NAMRBD_WIREV2_HDR_LEN) {
			ret = -EINVAL;
			goto out;
		}
		put_unaligned_le32((u32)n, &hdr[64]);
		hello_len = NAMRBD_WIREV2_HDR_LEN + (size_t)n;
	}

	ret = namrbd_transport_connect(namrbd_transport_endpoint_for_path(path), &sock);
	if (ret < 0)
		goto out;
	ret = namrbd_transport_send_all(sock, hello_buf, hello_len);
	if (ret)
		goto out;
	hello_ack_buf = kzalloc(NAMRBD_WIREV2_RESP_LEN + 512, GFP_KERNEL);
	if (!hello_ack_buf) {
		ret = -ENOMEM;
		goto out;
	}
	ret = namrbd_transport_recv_all(sock, hello_ack_buf, NAMRBD_WIREV2_RESP_LEN);
	if (ret)
		goto out;
	ret = namrbd_wirev2_validate_response_header(hello_ack_buf, NAMRBD_WIREV2_OP_HELLO_ACK,
						     request_id, dev->volume_id,
						     dev->generation, 0, 0);
	if (ret)
		goto out;
	resp_payload_len = get_unaligned_le32(&hello_ack_buf[64]);
	resp_auth_len = get_unaligned_le16(&hello_ack_buf[68]);
	hello_ack_len = NAMRBD_WIREV2_RESP_LEN + resp_payload_len + resp_auth_len;
	if (hello_ack_len > NAMRBD_WIREV2_RESP_LEN + 512) {
		ret = -EMSGSIZE;
		goto out;
	}
	if (resp_payload_len > 0) {
		ret = namrbd_transport_recv_all(sock, hello_ack_buf + NAMRBD_WIREV2_RESP_LEN,
						resp_payload_len + resp_auth_len);
		if (ret)
			goto out;
	}
	ret = namrbd_wirev2_hello_ack_parse((const char *)(hello_ack_buf + NAMRBD_WIREV2_RESP_LEN),
					    resp_payload_len, &session_id, server_nonce,
					    sizeof(server_nonce));
	if (ret)
		goto out;
	ret = namrbd_wirev2_derive_session_key(derivation_key, derivation_key_len,
					      dev->dataplane_token, token_len,
					      client_nonce, server_nonce, session_id, session_key);
	if (ret)
		goto out;

	req_len = NAMRBD_WIREV2_HDR_LEN + (op == REQ_OP_WRITE ? payload_len : 0) +
		  NAMRBD_WIREV2_AUTH_TAG_LEN;
	req_buf = kzalloc(req_len, GFP_KERNEL);
	if (!req_buf) {
		ret = -ENOMEM;
		goto out;
	}
	{
		u8 *hdr = req_buf;
		u32 plen = (op == REQ_OP_WRITE) ? payload_len : 0;

		namrbd_wirev2_encode_header(hdr, wire_op, 0, request_id, dev->volume_id,
					   dev->generation, session_id, 1,
					   offset_bytes, length_bytes, NAMRBD_WIREV2_AUTH_TAG_LEN);
		if (plen)
			memcpy(req_buf + NAMRBD_WIREV2_HDR_LEN, payload, plen);
		ret = namrbd_wirev2_compute_auth_tag(session_key, hdr,
						     plen ? payload : NULL, plen,
						     req_buf + NAMRBD_WIREV2_HDR_LEN + plen);
		if (ret)
			goto out;
	}
	ret = namrbd_transport_send_all(sock, req_buf, req_len);
	if (ret)
		goto out;

	resp_payload_len = 0;
	resp_len = NAMRBD_WIREV2_RESP_LEN;
	max_resp_payload_len = (op == REQ_OP_READ) ? blk_rq_bytes(rq) : 0;
	resp_buf = kzalloc(NAMRBD_WIREV2_RESP_LEN + max_resp_payload_len +
			   NAMRBD_WIREV2_AUTH_TAG_LEN, GFP_KERNEL);
	if (!resp_buf) {
		ret = -ENOMEM;
		goto out;
	}
	ret = namrbd_transport_recv_all(sock, resp_buf, NAMRBD_WIREV2_RESP_LEN);
	if (ret)
		goto out;
	ret = namrbd_wirev2_validate_response_header(resp_buf, expected_resp_op,
						     request_id, dev->volume_id,
						     dev->generation, session_id, 1);
	if (ret)
		goto out;
	resp_payload_len = get_unaligned_le32(&resp_buf[64]);
	resp_auth_len = get_unaligned_le16(&resp_buf[68]);
	resp_len = NAMRBD_WIREV2_RESP_LEN + resp_payload_len + resp_auth_len;
	if (resp_len > NAMRBD_WIREV2_RESP_LEN + max_resp_payload_len +
		       NAMRBD_WIREV2_AUTH_TAG_LEN) {
		ret = -EMSGSIZE;
		goto out;
	}
	if (resp_payload_len + resp_auth_len > 0) {
		ret = namrbd_transport_recv_all(sock, resp_buf + NAMRBD_WIREV2_RESP_LEN,
						resp_payload_len + resp_auth_len);
		if (ret)
			goto out;
	}
	if (wire_status_out)
		*wire_status_out = get_unaligned_le32(&resp_buf[76]);
	ret = namrbd_status_to_errno((s32)get_unaligned_le32(&resp_buf[76]));
	if (ret)
		goto out;
	{
		u8 expected_tag[NAMRBD_WIREV2_AUTH_TAG_LEN];

		if (resp_auth_len != NAMRBD_WIREV2_AUTH_TAG_LEN) {
			ret = -EBADMSG;
			goto out;
		}
		ret = namrbd_wirev2_compute_auth_tag(session_key, resp_buf,
						     resp_buf + NAMRBD_WIREV2_RESP_LEN,
						     resp_payload_len, expected_tag);
		if (ret)
			goto out;
		if (memcmp(expected_tag, resp_buf + NAMRBD_WIREV2_RESP_LEN + resp_payload_len,
			   NAMRBD_WIREV2_AUTH_TAG_LEN) != 0) {
			ret = -EACCES;
			goto out;
		}
	}
	if (op == REQ_OP_READ && resp_payload_len > 0) {
		ret = namrbd_rw_copy_read_payload(rq, resp_buf + NAMRBD_WIREV2_RESP_LEN,
						   resp_payload_len);
		if (ret)
			goto out;
	}
out:
	kfree(resp_buf);
	kfree(req_buf);
	kfree(hello_ack_buf);
	kfree(hello_buf);
	if (sock)
		sock_release(sock);
	return ret;
}

static int namrbd_data_path_submit_async(struct namrbd_blk_dev *dev, struct request *rq,
					 struct namrbd_path *path, u32 lane_id,
					 bool retry, ulong tried_mask, u32 attempt)
{
	u8 header[NAMRBD_WIRE_HDR_LEN];
	u8 *payload = NULL;
	u32 payload_len = 0;
	u64 request_id;
	u32 flags = 0;
	namrbd_req_op_t op = (namrbd_req_op_t)req_op(rq);
	u32 wire_op;
	u32 request_length;
	int ret;
	struct namrbd_pending_req *pending = NULL;
	struct socket *sock;
	struct socket *close_sock = NULL;
	struct task_struct *close_task = NULL;
	bool pending_added = false;
	bool sem_acquired = false;
	bool accounted = false;
	u64 accounted_bytes = namrbd_request_inflight_bytes(rq, op);

	if (!namrbd_transport_endpoint_for_path(path))
		return -EOPNOTSUPP;
	if (!dev->attached)
		return -ENODEV;
	if (dev->fail_path_id >= 0 && path && path->path_id == (u32)dev->fail_path_id)
		return -EIO;
	if (blk_rq_bytes(rq) > namrbd_max_io_size_for_op(dev, op))
		return -EMSGSIZE;
	if (atomic_read(&dev->data_inflight_reqs) >= dev->max_inflight_requests)
		return -EBUSY;
	if ((u64)atomic64_read(&dev->data_inflight_bytes) + accounted_bytes >
	    dev->max_inflight_bytes)
		return -EBUSY;

	ret = namrbd_rw_collect_payload(rq, op, &payload, &payload_len);
	if (ret)
		return ret;

	pending = kzalloc(sizeof(*pending), GFP_KERNEL);
	if (!pending) {
		ret = -ENOMEM;
		goto out_free_payload;
	}

	atomic_inc(&dev->data_inflight_reqs);
	atomic64_add(accounted_bytes, &dev->data_inflight_bytes);
	accounted = true;

	request_id = atomic64_inc_return(&dev->request_seq);
	wire_op = namrbd_wire_op_for_req(op);
	if (!wire_op) {
		ret = -EOPNOTSUPP;
		goto out_release_resources;
	}
	request_length = namrbd_wire_request_length(rq, op, payload_len);
	namrbd_wire_encode_header(header, wire_op, flags, request_id, dev->volume_id,
				  dev->generation, (u64)blk_rq_pos(rq) << 9,
				  request_length);

	if (down_trylock(&path->outstanding_sem)) {
		ret = -EBUSY;
		goto out_release_resources;
	}
	sem_acquired = true;
	INIT_LIST_HEAD(&pending->list);
	init_completion(&pending->done);
	pending->dev = dev;
	pending->path = path;
	pending->rq = rq;
	pending->request_id = request_id;
	pending->volume_id = dev->volume_id;
	pending->generation = dev->generation;
	pending->start_ns = ktime_get_ns();
	pending->lane_id = lane_id;
	pending->expected_op = namrbd_wire_response_op(wire_op);
	pending->op = op;
	pending->max_resp_len = (op == REQ_OP_READ) ? blk_rq_bytes(rq) : 0;
	pending->tried_mask = tried_mask;
	pending->attempt = attempt;
	pending->err = 0;
	pending->wire_status = 0;
	pending->async = true;
	pending->retry = retry;
	pending->sem_acquired = true;
	pending->accounted = true;
	pending->accounted_bytes = accounted_bytes;
	pending->completed = false;
	pending->processing = false;

	mutex_lock(&path->io_lock);
	ret = namrbd_path_ensure_socket_locked(path);
	if (ret < 0)
		goto out_unlock;

	sock = READ_ONCE(path->sock);
	if (!sock) {
		ret = -ENOTCONN;
		goto out_unlock;
	}
	namrbd_path_add_pending(path, pending);
	pending_added = true;
	ret = namrbd_transport_send_all(sock, header, sizeof(header));
	if (ret)
		goto out_reset_socket;
	if (payload_len) {
		ret = namrbd_transport_send_all(sock, payload, payload_len);
		if (ret)
			goto out_reset_socket;
	}
	mutex_unlock(&path->io_lock);
	kfree(payload);
	return 0;

out_reset_socket:
	if (pending_added) {
		namrbd_path_remove_pending(path, pending);
		pending_added = false;
	}
	if (ret)
		namrbd_path_detach_socket_locked(path, &close_sock, &close_task);
out_unlock:
	mutex_unlock(&path->io_lock);
	if (ret)
		namrbd_path_fail_pending(path, ret);
	namrbd_path_finish_socket_close(close_sock, close_task);
	if (pending_added)
		namrbd_path_remove_pending(path, pending);
out_release_resources:
	if (sem_acquired)
		up(&path->outstanding_sem);
	if (accounted) {
		atomic_dec(&dev->data_inflight_reqs);
		atomic64_sub(accounted_bytes, &dev->data_inflight_bytes);
	}
	kfree(pending);
out_free_payload:
	kfree(payload);
	return ret;
}

static int namrbd_data_path_request_errno(struct namrbd_blk_dev *dev, struct request *rq,
					  struct namrbd_path *path, u32 *wire_status_out)
{
	u8 header[NAMRBD_WIRE_HDR_LEN];
	u8 *payload = NULL;
	u32 payload_len = 0;
	u64 request_id;
	u32 flags = 0;
	namrbd_req_op_t op = (namrbd_req_op_t)req_op(rq);
	u32 wire_op;
	u32 request_length;
	int ret;
	u32 wire_status = 0;
	struct namrbd_pending_req pending;
	struct socket *sock;
	struct socket *close_sock = NULL;
	struct task_struct *close_task = NULL;
	bool pending_added = false;
	bool sem_acquired = false;
	u64 accounted_bytes = namrbd_request_inflight_bytes(rq, op);

	if (!namrbd_transport_endpoint_for_path(path))
		return namrbd_rw_request(dev, rq, path) == BLK_STS_OK ? 0 : -EIO;
	if (!dev->attached)
		return -ENODEV;
	if (dev->fail_path_id >= 0 && path && path->path_id == (u32)dev->fail_path_id)
		return -EIO;
	if (blk_rq_bytes(rq) > namrbd_max_io_size_for_op(dev, op))
		return -EMSGSIZE;
	if (atomic_read(&dev->data_inflight_reqs) >= dev->max_inflight_requests)
		return -EBUSY;
	if ((u64)atomic64_read(&dev->data_inflight_bytes) + accounted_bytes >
	    dev->max_inflight_bytes)
		return -EBUSY;

	ret = namrbd_rw_collect_payload(rq, op, &payload, &payload_len);
	if (ret)
		return ret;

	atomic_inc(&dev->data_inflight_reqs);
	atomic64_add(accounted_bytes, &dev->data_inflight_bytes);

	if (dev->dataplane_auth_mode[0] &&
	    strcmp(dev->dataplane_auth_mode, "token-hmac-v1") == 0) {
		mutex_lock(&path->io_lock);
		ret = namrbd_data_path_request_v2(dev, rq, path, payload, payload_len,
						  op, wire_status_out);
		mutex_unlock(&path->io_lock);
		goto out_account;
	}

	request_id = atomic64_inc_return(&dev->request_seq);
	wire_op = namrbd_wire_op_for_req(op);
	if (!wire_op) {
		ret = -EOPNOTSUPP;
		goto out_account;
	}
	request_length = namrbd_wire_request_length(rq, op, payload_len);
	namrbd_wire_encode_header(header, wire_op, flags, request_id, dev->volume_id,
				  dev->generation, (u64)blk_rq_pos(rq) << 9,
				  request_length);

	if (down_trylock(&path->outstanding_sem)) {
		ret = -EBUSY;
		goto out_account;
	}
	sem_acquired = true;
	INIT_LIST_HEAD(&pending.list);
	init_completion(&pending.done);
	pending.dev = dev;
	pending.path = path;
	pending.rq = rq;
	pending.request_id = request_id;
	pending.volume_id = dev->volume_id;
	pending.generation = dev->generation;
	pending.start_ns = ktime_get_ns();
	pending.lane_id = NAMRBD_LANE_ID_NONE;
	pending.expected_op = namrbd_wire_response_op(wire_op);
	pending.op = op;
	pending.max_resp_len = (op == REQ_OP_READ) ? blk_rq_bytes(rq) : 0;
	pending.err = 0;
	pending.wire_status = 0;
	pending.async = false;
	pending.retry = false;
	pending.sem_acquired = true;
	pending.accounted = true;
	pending.accounted_bytes = accounted_bytes;
	pending.completed = false;
	pending.processing = false;

	mutex_lock(&path->io_lock);
	ret = namrbd_path_ensure_socket_locked(path);
	if (ret < 0)
		goto out_unlock;

	sock = READ_ONCE(path->sock);
	if (!sock) {
		ret = -ENOTCONN;
		goto out_unlock;
	}
	namrbd_path_add_pending(path, &pending);
	pending_added = true;
	ret = namrbd_transport_send_all(sock, header, sizeof(header));
	if (ret)
		goto out_reset_socket;
	if (payload_len) {
		ret = namrbd_transport_send_all(sock, payload, payload_len);
		if (ret)
			goto out_reset_socket;
	}
	mutex_unlock(&path->io_lock);

	wait_for_completion(&pending.done);
	ret = pending.err;
	wire_status = pending.wire_status;
	goto out_account;

out_reset_socket:
	if (ret)
		namrbd_path_detach_socket_locked(path, &close_sock, &close_task);
out_unlock:
	mutex_unlock(&path->io_lock);
	if (ret)
		namrbd_path_fail_pending(path, ret);
	namrbd_path_finish_socket_close(close_sock, close_task);
	if (pending_added && !pending.completed)
		wait_for_completion(&pending.done);
	ret = ret ?: pending.err;
	wire_status = pending.wire_status;
out_account:
	if (pending_added)
		namrbd_path_remove_pending(path, &pending);
	if (sem_acquired)
		up(&path->outstanding_sem);
	atomic_dec(&dev->data_inflight_reqs);
	atomic64_sub(accounted_bytes, &dev->data_inflight_bytes);
	if (wire_status_out)
		*wire_status_out = wire_status;
	kfree(payload);
	return ret;
}

static blk_status_t namrbd_rw_request(struct namrbd_blk_dev *dev, struct request *rq,
				      struct namrbd_path *path)
{
	struct req_iterator iter;
	struct bio_vec bvec;
	u64 pos = (u64)blk_rq_pos(rq) << 9;
	u32 bytes = blk_rq_bytes(rq);
	namrbd_req_op_t op = (namrbd_req_op_t)req_op(rq);
	unsigned long flags;

	if (!dev->data)
		return BLK_STS_IOERR;
	if (pos + bytes > dev->size_bytes)
		return BLK_STS_IOERR;

	if (op == REQ_OP_DISCARD || op == REQ_OP_WRITE_ZEROES) {
		spin_lock_irqsave(&dev->data_lock, flags);
		memset(dev->data + pos, 0, bytes);
		spin_unlock_irqrestore(&dev->data_lock, flags);
		if (dev->fail_path_id >= 0 && path && path->path_id == dev->fail_path_id)
			return BLK_STS_IOERR;
		return BLK_STS_OK;
	}

	rq_for_each_segment(bvec, rq, iter) {
		void *kaddr;
		u64 len = bvec.bv_len;
		u8 *dst;
		u8 *src;

		kaddr = kmap_local_page(bvec.bv_page);
		if (!kaddr)
			return BLK_STS_IOERR;

		spin_lock_irqsave(&dev->data_lock, flags);
		if (op == REQ_OP_READ) {
			dst = (u8 *)kaddr + bvec.bv_offset;
			src = dev->data + pos;
			memcpy(dst, src, len);
		} else {
			dst = dev->data + pos;
			src = (u8 *)kaddr + bvec.bv_offset;
			memcpy(dst, src, len);
		}
		spin_unlock_irqrestore(&dev->data_lock, flags);

		kunmap_local(kaddr);
		pos += len;
	}

	if (dev->fail_path_id >= 0 && path && path->path_id == dev->fail_path_id)
		return BLK_STS_IOERR;

	return BLK_STS_OK;
}

static blk_status_t namrbd_queue_rq(struct blk_mq_hw_ctx *hctx,
				    const struct blk_mq_queue_data *bd)
{
	struct request *rq = bd->rq;
	struct namrbd_blk_dev *dev = rq->q->queuedata;
	blk_status_t st = BLK_STS_OK;
	namrbd_req_op_t op = (namrbd_req_op_t)req_op(rq);
	struct namrbd_path *path = NULL;
	ulong tried_mask = 0;
	u32 active_limit;
	u32 lane_id = NAMRBD_LANE_ID_NONE;
	int attempt;
	bool requeued_no_path = false;
	bool requeued_resource = false;
	bool resource_busy = false;

	blk_mq_start_request(rq);
	atomic64_inc(&dev->total_reqs);
	if (op == REQ_OP_READ)
		atomic64_inc(&dev->read_reqs);
	if (op == REQ_OP_WRITE)
		atomic64_inc(&dev->write_reqs);
	if (op == REQ_OP_DISCARD)
		atomic64_inc(&dev->discard_reqs);
	if (op == REQ_OP_WRITE_ZEROES)
		atomic64_inc(&dev->write_zeroes_reqs);
	NAMRBD_TRACE("device=%u submit op=%u sector=%llu bytes=%u\n",
		     dev->device_id, op, (unsigned long long)blk_rq_pos(rq),
		     blk_rq_bytes(rq));

	switch (op) {
	case REQ_OP_READ:
	case REQ_OP_WRITE:
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		if (!dev->attached) {
			if (namrbd_requeue_no_path(dev, rq, op, tried_mask,
						   NAMRBD_NO_PATH_DETACHED))
				requeued_no_path = true;
			else
				st = namrbd_fail_no_path(dev, op, tried_mask,
							 NAMRBD_NO_PATH_DETACHED);
			break;
		}
		if (namrbd_try_complete_zero_like_from_zero_map(dev, rq, op))
			return BLK_STS_OK;
		if (dev->dataplane_auth_mode[0] &&
		    strcmp(dev->dataplane_auth_mode, "token-hmac-v1") == 0 &&
		    (op == REQ_OP_DISCARD || op == REQ_OP_WRITE_ZEROES)) {
			st = BLK_STS_NOTSUPP;
			break;
		}
		lane_id = namrbd_request_lane_id(dev, hctx, rq);
		active_limit = dev->nr_paths;
		if (active_limit == 0) {
			if (namrbd_requeue_no_path(dev, rq, op, tried_mask,
						   NAMRBD_NO_PATH_PLAN_EMPTY))
				requeued_no_path = true;
			else
				st = namrbd_fail_no_path(dev, op, tried_mask,
							 NAMRBD_NO_PATH_PLAN_EMPTY);
			break;
		}
		for (attempt = 0; attempt < active_limit; attempt++) {
			u64 start_ns;
			u64 lat_ns;
			int req_err;
			int slot;
			u32 wire_status = 0;

			path = namrbd_pick_path_for_lane(dev, op, lane_id, tried_mask);
			if (!path) {
				if (namrbd_requeue_no_path(dev, rq, op, tried_mask,
							   NAMRBD_NO_PATH_NONE))
					requeued_no_path = true;
				else
					st = namrbd_fail_no_path(dev, op, tried_mask,
								 NAMRBD_NO_PATH_NONE);
				break;
			}

			slot = (int)(path - dev->paths);
			if (slot < 0 || slot >= dev->nr_paths) {
				st = BLK_STS_IOERR;
				break;
			}
			tried_mask |= (1UL << slot);
			atomic_inc(&path->inflight);
			if (lane_id != NAMRBD_LANE_ID_NONE && lane_id < NAMRBD_MAX_PATHS)
				atomic64_inc(&dev->lane_dispatch_reqs[lane_id]);
			NAMRBD_TRACE("device=%u pick_lane=%u path=%u state=%s inflight=%d attempt=%d\n",
				     dev->device_id, lane_id, path->path_id,
				     namrbd_path_state_str(path->state),
				     atomic_read(&path->inflight), attempt);
			start_ns = ktime_get_ns();
			if (!dev->dataplane_auth_mode[0] &&
			    namrbd_transport_endpoint_for_path(path)) {
				req_err = namrbd_data_path_submit_async(dev, rq, path,
								       lane_id,
								       attempt > 0,
								       tried_mask,
								       attempt);
				if (!req_err) {
					namrbd_untrack_resource_requeue(dev, rq);
					return BLK_STS_OK;
				}
			} else {
				req_err = namrbd_data_path_request_errno(dev, rq, path,
									&wire_status);
			}
			st = req_err ? BLK_STS_IOERR : BLK_STS_OK;
			lat_ns = ktime_get_ns() - start_ns;
			atomic_dec(&path->inflight);
			if (namrbd_datapath_resource_busy(req_err)) {
				resource_busy = true;
				st = BLK_STS_RESOURCE;
				NAMRBD_TRACE_INFO("device=%u datapath_resource_busy op=%u sector=%llu bytes=%u lane=%u attempt=%d path=%u req_err=%d errno=%d tried=0x%llx latency_ns=%llu inflight=%d data_inflight_reqs=%d data_inflight_bytes=%lld\n",
						  dev->device_id, op,
						  (unsigned long long)blk_rq_pos(rq),
						  blk_rq_bytes(rq), lane_id, attempt,
						  path->path_id, req_err, -req_err,
						  (unsigned long long)tried_mask,
						  (unsigned long long)lat_ns,
						  atomic_read(&path->inflight),
						  atomic_read(&dev->data_inflight_reqs),
						  atomic64_read(&dev->data_inflight_bytes));
				break;
			}
			namrbd_path_complete(path, lat_ns, attempt > 0);

			if (st == BLK_STS_OK) {
				namrbd_zero_map_update_after_success(dev, rq, op);
				dev->last_completed_path_id = path->path_id;
				if (dev->no_path_state != NAMRBD_NO_PATH_INACTIVE) {
					unsigned int no_path_wait_ms =
						dev->no_path_since_jiffies ?
							jiffies_to_msecs(jiffies - dev->no_path_since_jiffies) :
							0;

					NAMRBD_TRACE_INFO("device=%u no_path_recovered op=%u path=%u lane=%u attempted_mask=0x%llx datapath_latency_ns=%llu no_path_wait_ms=%u queued_reqs_total=%lld requeued_reqs_total=%lld failed_reqs_total=%lld\n",
							  dev->device_id, op, path->path_id,
							  lane_id,
							  (unsigned long long)tried_mask,
							  (unsigned long long)lat_ns,
							  no_path_wait_ms,
							  atomic64_read(&dev->no_path_queued_reqs),
							  atomic64_read(&dev->no_path_requeued_reqs),
							  atomic64_read(&dev->no_path_failed_reqs));
					dev->no_path_state = NAMRBD_NO_PATH_INACTIVE;
					dev->no_path_since_jiffies = 0;
					dev->no_path_retry_deadline_jiffies = 0;
					atomic64_inc(&dev->no_path_recovered_reqs);
				}
				namrbd_path_mark_success(dev, path);
				break;
			}
			dev->last_failed_path_id = path->path_id;
			NAMRBD_TRACE_INFO("device=%u datapath_failure op=%u sector=%llu bytes=%u lane=%u attempt=%d path=%u pre_state=%s req_err=%d errno=%d wire_status=%u(%s) tried=0x%llx latency_ns=%llu inflight=%d pre_consecutive_errors=%u no_path_state=%s retry_mode=%s\n",
					  dev->device_id, op,
					  (unsigned long long)blk_rq_pos(rq),
					  blk_rq_bytes(rq), lane_id, attempt,
					  path->path_id,
					  namrbd_path_state_str(path->state),
					  req_err, -req_err, wire_status,
					  namrbd_wire_status_str(wire_status),
					  (unsigned long long)tried_mask,
					  (unsigned long long)lat_ns,
					  atomic_read(&path->inflight),
					  path->consecutive_errors,
					  namrbd_no_path_state_str(dev->no_path_state),
					  namrbd_no_path_retry_mode_str(dev->no_path_retry_mode));
			namrbd_path_mark_failure(dev, path, -req_err, wire_status);
			NAMRBD_TRACE_INFO("device=%u path_failure_marked path=%u post_state=%s consecutive_errors=%u last_errno=%u last_wire_status=%u(%s)\n",
					  dev->device_id, path->path_id,
					  namrbd_path_state_str(path->state),
					  path->consecutive_errors,
					  path->last_errno, path->last_wire_status,
					  namrbd_wire_status_str(path->last_wire_status));
			if (req_err == -EAGAIN || req_err == -ETIMEDOUT)
				atomic64_inc(&dev->timeout_reqs);
			if (attempt + 1 < active_limit) {
				dev->last_failover_from_path_id = path->path_id;
				dev->last_failover_to_path_id = NAMRBD_PATH_ID_NONE;
				atomic64_inc(&dev->path_failover_reqs);
			}
			atomic64_inc(&dev->retry_reqs);
		}
		if (st != BLK_STS_OK && resource_busy) {
			if (namrbd_requeue_datapath_resource(dev, rq, op, tried_mask,
							     "submit-busy"))
				requeued_resource = true;
			else
				st = BLK_STS_IOERR;
		}
		if (!requeued_resource && st != BLK_STS_OK &&
		    namrbd_classify_no_path(dev, op, tried_mask, NULL) !=
		    NAMRBD_NO_PATH_NONE &&
		    namrbd_requeue_no_path(dev, rq, op, tried_mask,
					   NAMRBD_NO_PATH_NONE))
			requeued_no_path = true;
		break;
	case REQ_OP_FLUSH:
		if (!dev->attached) {
			if (namrbd_requeue_no_path(dev, rq, op, tried_mask,
						   NAMRBD_NO_PATH_DETACHED))
				requeued_no_path = true;
			else
				st = namrbd_fail_no_path(dev, op, tried_mask,
							 NAMRBD_NO_PATH_DETACHED);
			break;
		}
		if (namrbd_classify_no_path(dev, op, tried_mask, NULL) !=
		    NAMRBD_NO_PATH_NONE) {
			if (namrbd_requeue_no_path(dev, rq, op, tried_mask,
						   NAMRBD_NO_PATH_NONE))
				requeued_no_path = true;
			else
				st = namrbd_fail_no_path(dev, op, tried_mask,
							 NAMRBD_NO_PATH_NONE);
		} else if (namrbd_requeue_flush_until_datapath_quiesced(dev, rq,
									 tried_mask)) {
			requeued_resource = true;
		} else {
			st = BLK_STS_OK;
		}
		break;
	default:
		st = BLK_STS_NOTSUPP;
		break;
	}

	if (!requeued_resource &&
	    namrbd_untrack_resource_requeue(dev, rq))
		namrbd_kick_datapath_resource_requeue(dev);
	if (requeued_no_path || requeued_resource)
		return BLK_STS_OK;

	if (st == BLK_STS_OK)
		atomic64_inc(&dev->completed_reqs);
	else
		atomic64_inc(&dev->failed_reqs);
	NAMRBD_TRACE("device=%u complete op=%u status=%u\n", dev->device_id, op, st);
	blk_mq_end_request(rq, st);
	return BLK_STS_OK;
}

static const struct blk_mq_ops namrbd_mq_ops = {
	.queue_rq = namrbd_queue_rq,
};

static int namrbd_init_paths(struct namrbd_blk_dev *dev)
{
	int i;

	if (nr_paths < 1 || nr_paths > NAMRBD_MAX_PATHS)
		return -EINVAL;

	dev->paths = kcalloc(nr_paths, sizeof(*dev->paths), GFP_KERNEL);
	if (!dev->paths)
		return -ENOMEM;
	dev->nr_paths = nr_paths;
	dev->active_path_count = nr_paths;

	for (i = 0; i < dev->nr_paths; i++) {
		struct namrbd_path *p = &dev->paths[i];

		p->path_id = i;
		p->priority = 0;
		p->gateway_id[0] = '\0';
		p->endpoint.port = 0;
		p->endpoint.address[0] = '\0';
		p->endpoint.server_name[0] = '\0';
		p->endpoint.use_tls = false;
		p->state = NAMRBD_PATH_UP;
		if (down_mask & (1UL << i))
			p->state = NAMRBD_PATH_DOWN;
		else if (draining_mask & (1UL << i))
			p->state = NAMRBD_PATH_DRAINING;
		else if (degraded_mask & (1UL << i))
			p->state = NAMRBD_PATH_DEGRADED;
		p->configured_state = p->state;
		atomic_set(&p->inflight, 0);
		p->consecutive_errors = 0;
		p->last_errno = 0;
		p->last_wire_status = 0;
		p->state_changes = 0;
		p->last_transition_jiffies = jiffies;
		p->ewma_latency_ns = 1000000;
		p->connection_opens = 0;
		p->connection_resets = 0;
		p->sock = NULL;
		p->recv_task = NULL;
		p->outstanding_limit = namrbd_per_path_outstanding_limit();
		sema_init(&p->outstanding_sem, p->outstanding_limit);
		spin_lock_init(&p->pending_lock);
		INIT_LIST_HEAD(&p->pending_reqs);
		p->pending_high_water = 0;
		p->submitted = 0;
		mutex_init(&p->io_lock);
		spin_lock_init(&p->lock);
	}

	if (!strcmp(sched_policy, "rr"))
		dev->policy = NAMRBD_SCHED_RR;
	else if (!strcmp(sched_policy, "ewma"))
		dev->policy = NAMRBD_SCHED_EWMA;
	else
		dev->policy = NAMRBD_SCHED_LEAST_INFLIGHT;

	dev->rr_cursor = 0;
	for (i = 0; i < NAMRBD_MAX_PATHS; i++)
		dev->lane_preferred_path_ids[i] = NAMRBD_PATH_ID_NONE;
	dev->lane_remap_count = 0;
	dev->last_lane_remapped_lanes = 0;
	dev->last_lane_remap_jiffies = 0;
	dev->last_lane_remap_reason[0] = '\0';
	dev->last_selected_lane_id = NAMRBD_LANE_ID_NONE;
	dev->last_selected_path_id = NAMRBD_PATH_ID_NONE;
	dev->last_completed_path_id = NAMRBD_PATH_ID_NONE;
	dev->last_failed_path_id = NAMRBD_PATH_ID_NONE;
	dev->last_failover_from_path_id = NAMRBD_PATH_ID_NONE;
	dev->last_failover_to_path_id = NAMRBD_PATH_ID_NONE;
	dev->last_no_path_reason = NAMRBD_NO_PATH_NONE;
	dev->last_no_path_op = 0;
	dev->last_no_path_eligible_paths = 0;
	dev->last_no_path_tried_mask = 0;
	dev->last_no_path_jiffies = 0;
	dev->no_path_state = NAMRBD_NO_PATH_INACTIVE;
	dev->no_path_since_jiffies = 0;
	dev->no_path_retry_deadline_jiffies = 0;
	dev->last_no_path_wakeup_jiffies = 0;
	for (i = 0; i < NAMRBD_MAX_PATHS; i++)
		atomic64_set(&dev->lane_dispatch_reqs[i], 0);
	namrbd_refresh_lane_map(dev, "init_paths");
	namrbd_refresh_queue_topology_target_control(dev);
	return 0;
}

static void namrbd_cleanup_paths(struct namrbd_blk_dev *dev)
{
	int i;

	if (dev && dev->paths) {
		for (i = 0; i < dev->nr_paths; i++)
			namrbd_path_close_socket(&dev->paths[i]);
	}
	kfree(dev->paths);
	dev->paths = NULL;
	dev->nr_paths = 0;
}

static ssize_t volume_state_show(struct device *d,
				 struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);
	int i;

	if (!dev || !dev->disk || !dev->attached)
		return scnprintf(buf, PAGE_SIZE, "DETACHED\n");
	for (i = 0; i < dev->nr_paths; i++) {
		if (dev->paths[i].state != NAMRBD_PATH_UP)
			return scnprintf(buf, PAGE_SIZE, "DEGRADED\n");
	}
	return scnprintf(buf, PAGE_SIZE, "ATTACHED\n");
}

static ssize_t size_bytes_show(struct device *d,
			       struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);

	if (!dev)
		return scnprintf(buf, PAGE_SIZE, "0\n");
	return scnprintf(buf, PAGE_SIZE, "%llu\n",
			 (unsigned long long)dev->size_bytes);
}

static ssize_t block_size_show(struct device *d,
			       struct device_attribute *attr, char *buf)
{
	return scnprintf(buf, PAGE_SIZE, "%u\n", NAMRBD_BLOCK_SIZE);
}

static ssize_t generation_show(struct device *d,
			       struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);

	if (!dev)
		return scnprintf(buf, PAGE_SIZE, "0\n");
	return scnprintf(buf, PAGE_SIZE, "%llu\n",
			 (unsigned long long)dev->generation);
}

static ssize_t dataplane_show(struct device *d,
			      struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);
	u32 limit;
	u32 i;
	ssize_t off = 0;

	if (!dev || !dev->paths)
		return scnprintf(buf, PAGE_SIZE, "disabled\n");
	limit = dev->active_path_count ? dev->active_path_count : dev->nr_paths;
	for (i = 0; i < limit && off < PAGE_SIZE; i++) {
		struct namrbd_path *path = &dev->paths[i];

		if (!path->endpoint.port || !path->endpoint.address[0])
			continue;
		off += scnprintf(buf + off, PAGE_SIZE - off,
				 "path_id=%u gateway_id=%s %s:%u zero_io=%u data_io=%u inflight_reqs=%u inflight_bytes=%llu\n",
				 path->path_id,
				 path->gateway_id[0] ? path->gateway_id : "-",
				 path->endpoint.address, path->endpoint.port,
				 dev->max_zero_like_io_size,
				 dev->max_data_io_size,
				 dev->max_inflight_requests,
				 (unsigned long long)dev->max_inflight_bytes);
	}
	if (off == 0)
		return scnprintf(buf, PAGE_SIZE, "disabled\n");
	return off;
}

static ssize_t dataplane_inflight_show(struct device *d,
				       struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);

	if (!dev)
		return scnprintf(buf, PAGE_SIZE, "reqs=0 bytes=0\n");
	return scnprintf(buf, PAGE_SIZE, "reqs=%d bytes=%lld\n",
			 atomic_read(&dev->data_inflight_reqs),
			 atomic64_read(&dev->data_inflight_bytes));
}

static ssize_t fail_path_id_show(struct device *d,
				 struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);

	if (!dev)
		return scnprintf(buf, PAGE_SIZE, "-1\n");
	return scnprintf(buf, PAGE_SIZE, "%d\n", dev->fail_path_id);
}

static ssize_t fail_path_id_store(struct device *d,
				  struct device_attribute *attr,
				  const char *buf, size_t count)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);
	int v;

	if (!dev)
		return -ENODEV;
	if (kstrtoint(buf, 10, &v))
		return -EINVAL;
	if (v < -1 || v >= dev->nr_paths)
		return -ERANGE;
	mutex_lock(&dev->state_lock);
	dev->fail_path_id = v;
	mutex_unlock(&dev->state_lock);
	return count;
}

static ssize_t active_policy_show(struct device *d,
				  struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);

	if (!dev)
		return scnprintf(buf, PAGE_SIZE, "unknown\n");
	return scnprintf(buf, PAGE_SIZE, "%s\n", namrbd_sched_policy_str(dev->policy));
}

static ssize_t path_states_show(struct device *d,
				struct device_attribute *attr, char *buf)
{
	struct namrbd_blk_dev *dev = namrbd_dev_from_disk_device(d);
	int i;
	ssize_t n = 0;

	if (!dev || !dev->paths)
		return scnprintf(buf, PAGE_SIZE, "none\n");

	for (i = 0; i < dev->nr_paths && n < PAGE_SIZE - 32; i++) {
		struct namrbd_path *p = &dev->paths[i];

		n += scnprintf(buf + n, PAGE_SIZE - n, "%d:%s ",
			       p->path_id, namrbd_path_state_str(p->state));
	}
	n += scnprintf(buf + n, PAGE_SIZE - n, "\n");
	return n;
}

static DEVICE_ATTR_RO(volume_state);
static DEVICE_ATTR_RO(size_bytes);
static DEVICE_ATTR_RO(block_size);
static DEVICE_ATTR_RO(generation);
static DEVICE_ATTR_RO(dataplane);
static DEVICE_ATTR_RO(dataplane_inflight);
static DEVICE_ATTR_RW(fail_path_id);
static DEVICE_ATTR_RO(active_policy);
static DEVICE_ATTR_RO(path_states);

static int namrbd_sysfs_create(struct namrbd_blk_dev *dev)
{
	int ret;
	struct device *disk_dev = disk_to_dev(dev->disk);

	ret = device_create_file(disk_dev, &dev_attr_volume_state);
	if (ret)
		return ret;
	ret = device_create_file(disk_dev, &dev_attr_size_bytes);
	if (ret)
		goto out_remove_volume_state;
	ret = device_create_file(disk_dev, &dev_attr_block_size);
	if (ret)
		goto out_remove_size_bytes;
	ret = device_create_file(disk_dev, &dev_attr_generation);
	if (ret)
		goto out_remove_block_size;
	ret = device_create_file(disk_dev, &dev_attr_dataplane);
	if (ret)
		goto out_remove_generation;
	ret = device_create_file(disk_dev, &dev_attr_dataplane_inflight);
	if (ret)
		goto out_remove_dataplane;
	ret = device_create_file(disk_dev, &dev_attr_fail_path_id);
	if (ret)
		goto out_remove_dataplane_inflight;
	ret = device_create_file(disk_dev, &dev_attr_active_policy);
	if (ret)
		goto out_remove_fail_path_id;
	ret = device_create_file(disk_dev, &dev_attr_path_states);
	if (ret)
		goto out_remove_active_policy;
	return 0;

out_remove_active_policy:
	device_remove_file(disk_dev, &dev_attr_active_policy);
out_remove_fail_path_id:
	device_remove_file(disk_dev, &dev_attr_fail_path_id);
out_remove_dataplane_inflight:
	device_remove_file(disk_dev, &dev_attr_dataplane_inflight);
out_remove_dataplane:
	device_remove_file(disk_dev, &dev_attr_dataplane);
out_remove_generation:
	device_remove_file(disk_dev, &dev_attr_generation);
out_remove_block_size:
	device_remove_file(disk_dev, &dev_attr_block_size);
out_remove_size_bytes:
	device_remove_file(disk_dev, &dev_attr_size_bytes);
out_remove_volume_state:
	device_remove_file(disk_dev, &dev_attr_volume_state);
	return ret;
}

static void namrbd_sysfs_remove(struct namrbd_blk_dev *dev)
{
	struct device *disk_dev;

	if (!dev || !dev->disk)
		return;

	disk_dev = disk_to_dev(dev->disk);
	device_remove_file(disk_dev, &dev_attr_path_states);
	device_remove_file(disk_dev, &dev_attr_active_policy);
	device_remove_file(disk_dev, &dev_attr_fail_path_id);
	device_remove_file(disk_dev, &dev_attr_dataplane_inflight);
	device_remove_file(disk_dev, &dev_attr_dataplane);
	device_remove_file(disk_dev, &dev_attr_generation);
	device_remove_file(disk_dev, &dev_attr_block_size);
	device_remove_file(disk_dev, &dev_attr_size_bytes);
	device_remove_file(disk_dev, &dev_attr_volume_state);
}

static void namrbd_collect_path_plan_summary(struct namrbd_blk_dev *dev,
					     u64 *down_mask_bits,
					     u64 *degraded_mask_bits,
					     u64 *draining_mask_bits,
					     u32 *up_paths,
					     u32 *degraded_paths,
					     u32 *down_paths,
					     u32 *draining_paths)
{
	u32 i;

	if (down_mask_bits)
		*down_mask_bits = 0;
	if (degraded_mask_bits)
		*degraded_mask_bits = 0;
	if (draining_mask_bits)
		*draining_mask_bits = 0;
	if (up_paths)
		*up_paths = 0;
	if (degraded_paths)
		*degraded_paths = 0;
	if (down_paths)
		*down_paths = 0;
	if (draining_paths)
		*draining_paths = 0;
	if (!dev || !dev->paths)
		return;

	for (i = 0; i < dev->nr_paths; i++) {
		switch (dev->paths[i].state) {
		case NAMRBD_PATH_DOWN:
			if (down_mask_bits)
				*down_mask_bits |= (1ULL << i);
			if (down_paths)
				(*down_paths)++;
			break;
		case NAMRBD_PATH_DEGRADED:
			if (degraded_mask_bits)
				*degraded_mask_bits |= (1ULL << i);
			if (degraded_paths)
				(*degraded_paths)++;
			break;
		case NAMRBD_PATH_DRAINING:
			if (draining_mask_bits)
				*draining_mask_bits |= (1ULL << i);
			if (draining_paths)
				(*draining_paths)++;
			break;
		case NAMRBD_PATH_UP:
		default:
			if (up_paths)
				(*up_paths)++;
			break;
		}
	}
}

static int namrbd_debugfs_stats_show(struct seq_file *m, void *v)
{
	struct namrbd_blk_dev *dev = m->private;
	u64 down_mask_bits;
	u64 degraded_mask_bits;
	u64 draining_mask_bits;
	u32 up_paths;
	u32 degraded_paths;
	u32 down_paths;
	u32 draining_paths;
	u32 zero_map_enabled;
	u32 zero_map_granule_bytes;
	u64 zero_map_granules;
	unsigned long zero_map_flags;

	if (!dev)
		return 0;

	mutex_lock(&dev->state_lock);
	namrbd_collect_path_plan_summary(dev, &down_mask_bits, &degraded_mask_bits,
					 &draining_mask_bits, &up_paths, &degraded_paths,
					 &down_paths, &draining_paths);
	spin_lock_irqsave(&dev->zero_map_lock, zero_map_flags);
	zero_map_enabled = dev->zero_map ? 1 : 0;
	zero_map_granule_bytes = dev->zero_map_granule_bytes;
	zero_map_granules = dev->zero_map_granules;
	spin_unlock_irqrestore(&dev->zero_map_lock, zero_map_flags);
	seq_printf(m, "device_id=%u\n", dev->device_id);
	seq_printf(m, "disk_name=%s\n", dev->disk_name);
	seq_printf(m, "attached=%u\n", dev->attached ? 1 : 0);
	seq_printf(m, "volume_id=%llu\n", (unsigned long long)dev->volume_id);
	seq_printf(m, "generation=%llu\n", (unsigned long long)dev->generation);
	seq_printf(m, "active_path_count=%u\n", dev->active_path_count);
	seq_printf(m, "active_lane_count=%u\n", dev->active_lane_count);
	seq_printf(m, "per_path_outstanding=%u\n", namrbd_per_path_outstanding_limit());
	seq_printf(m, "nr_hw_queues=%u\n", dev->tag_set.nr_hw_queues);
	seq_printf(m, "target_nr_hw_queues=%u\n", dev->target_nr_hw_queues);
	seq_printf(m, "queue_topology_generation=%llu\n",
		   (unsigned long long)dev->queue_topology_generation);
	seq_printf(m, "queue_topology_state=%s\n", dev->queue_topology_state);
	if (dev->chunk_size_bytes)
		seq_printf(m, "chunk_size_bytes=%u\n", dev->chunk_size_bytes);
	seq_printf(m, "max_io_size=%u\n", dev->max_io_size);
	seq_printf(m, "max_data_io_size=%u\n", dev->max_data_io_size);
	seq_printf(m, "max_zero_like_io_size=%u\n", dev->max_zero_like_io_size);
	seq_printf(m, "max_data_io_bytes=%u\n",
		   namrbd_max_io_sectors(dev->max_data_io_size) * NAMRBD_SECTOR_SIZE);
	seq_printf(m, "max_discard_bytes=%u\n",
		   namrbd_max_io_sectors(dev->max_zero_like_io_size) * NAMRBD_SECTOR_SIZE);
	seq_printf(m, "max_write_zeroes_bytes=%u\n",
		   namrbd_max_io_sectors(dev->max_zero_like_io_size) * NAMRBD_SECTOR_SIZE);
	seq_printf(m, "zero_map_enabled=%u\n", zero_map_enabled);
	seq_printf(m, "zero_map_granule_bytes=%u\n", zero_map_granule_bytes);
	seq_printf(m, "zero_map_granules=%llu\n",
		   (unsigned long long)zero_map_granules);
	seq_printf(m, "zero_map_local_skips=%lld\n",
		   atomic64_read(&dev->zero_map_local_skips));
	seq_printf(m, "zero_map_mark_zero_reqs=%lld\n",
		   atomic64_read(&dev->zero_map_mark_zero_reqs));
	seq_printf(m, "zero_map_mark_data_reqs=%lld\n",
		   atomic64_read(&dev->zero_map_mark_data_reqs));
	seq_printf(m, "sched_policy=%s\n", namrbd_sched_policy_str(dev->policy));
	seq_printf(m, "rr_cursor=%u\n", dev->rr_cursor);
	seq_printf(m, "lane_remap_count=%llu\n", (unsigned long long)dev->lane_remap_count);
	seq_printf(m, "last_lane_remapped_lanes=%u\n", dev->last_lane_remapped_lanes);
	if (dev->last_lane_remap_reason[0])
		seq_printf(m, "last_lane_remap_reason=%s\n", dev->last_lane_remap_reason);
	if (dev->last_lane_remap_jiffies)
		seq_printf(m, "last_lane_remap_jiffies=%lu\n", dev->last_lane_remap_jiffies);
	seq_printf(m, "applied_path_plan_revision=%llu\n",
		   (unsigned long long)dev->applied_path_plan_revision);
	if (dev->last_selected_lane_id != NAMRBD_LANE_ID_NONE)
		seq_printf(m, "last_selected_lane_id=%u\n", dev->last_selected_lane_id);
	if (dev->last_selected_path_id != NAMRBD_PATH_ID_NONE)
		seq_printf(m, "last_selected_path_id=%u\n", dev->last_selected_path_id);
	if (dev->last_completed_path_id != NAMRBD_PATH_ID_NONE)
		seq_printf(m, "last_completed_path_id=%u\n", dev->last_completed_path_id);
	if (dev->last_failed_path_id != NAMRBD_PATH_ID_NONE)
		seq_printf(m, "last_failed_path_id=%u\n", dev->last_failed_path_id);
	if (dev->last_failover_from_path_id != NAMRBD_PATH_ID_NONE)
		seq_printf(m, "last_failover_from_path_id=%u\n", dev->last_failover_from_path_id);
	if (dev->last_failover_to_path_id != NAMRBD_PATH_ID_NONE)
		seq_printf(m, "last_failover_to_path_id=%u\n", dev->last_failover_to_path_id);
	seq_printf(m, "no_path_reqs=%lld\n", atomic64_read(&dev->no_path_reqs));
	seq_printf(m, "no_path_retry_mode=%s\n",
		   namrbd_no_path_retry_mode_str(dev->no_path_retry_mode));
	seq_printf(m, "no_path_retry_seconds=%u\n", dev->no_path_retry_seconds);
	seq_printf(m, "no_path_requeue_delay_ms=%u\n", no_path_requeue_delay_ms);
	seq_printf(m, "no_path_max_queued_requests=%u\n",
		   no_path_max_queued_requests);
	seq_printf(m, "no_path_state=%s\n",
		   namrbd_no_path_state_str(dev->no_path_state));
	if (dev->no_path_since_jiffies)
		seq_printf(m, "no_path_since_jiffies=%lu\n",
			   dev->no_path_since_jiffies);
	if (dev->no_path_retry_deadline_jiffies)
		seq_printf(m, "no_path_retry_deadline_jiffies=%lu\n",
			   dev->no_path_retry_deadline_jiffies);
	if (dev->last_no_path_wakeup_jiffies)
		seq_printf(m, "last_no_path_wakeup_jiffies=%lu\n",
			   dev->last_no_path_wakeup_jiffies);
	seq_printf(m, "no_path_queued_reqs=%lld\n",
		   atomic64_read(&dev->no_path_queued_reqs));
	seq_printf(m, "no_path_requeued_reqs=%lld\n",
		   atomic64_read(&dev->no_path_requeued_reqs));
	seq_printf(m, "no_path_failed_reqs=%lld\n",
		   atomic64_read(&dev->no_path_failed_reqs));
	seq_printf(m, "no_path_recovered_reqs=%lld\n",
		   atomic64_read(&dev->no_path_recovered_reqs));
	seq_printf(m, "no_path_enter_count=%lld\n",
		   atomic64_read(&dev->no_path_enter_count));
	seq_printf(m, "last_no_path_reason=%s\n",
		   namrbd_no_path_reason_str(dev->last_no_path_reason));
	seq_printf(m, "last_no_path_op=%u\n", dev->last_no_path_op);
	seq_printf(m, "last_no_path_eligible_paths=%u\n",
		   dev->last_no_path_eligible_paths);
	seq_printf(m, "last_no_path_tried_mask=0x%llx\n",
		   (unsigned long long)dev->last_no_path_tried_mask);
	if (dev->last_no_path_jiffies)
		seq_printf(m, "last_no_path_jiffies=%lu\n", dev->last_no_path_jiffies);
	seq_printf(m, "down_mask=0x%llx\n", (unsigned long long)down_mask_bits);
	seq_printf(m, "degraded_mask=0x%llx\n", (unsigned long long)degraded_mask_bits);
	seq_printf(m, "draining_mask=0x%llx\n", (unsigned long long)draining_mask_bits);
	seq_printf(m, "up_paths=%u\n", up_paths);
	seq_printf(m, "degraded_paths=%u\n", degraded_paths);
	seq_printf(m, "down_paths=%u\n", down_paths);
	seq_printf(m, "draining_paths=%u\n", draining_paths);
	seq_printf(m, "total_reqs=%lld\n", atomic64_read(&dev->total_reqs));
	seq_printf(m, "read_reqs=%lld\n", atomic64_read(&dev->read_reqs));
	seq_printf(m, "write_reqs=%lld\n", atomic64_read(&dev->write_reqs));
	seq_printf(m, "discard_reqs=%lld\n", atomic64_read(&dev->discard_reqs));
	seq_printf(m, "write_zeroes_reqs=%lld\n",
		   atomic64_read(&dev->write_zeroes_reqs));
	seq_printf(m, "retry_reqs=%lld\n", atomic64_read(&dev->retry_reqs));
	seq_printf(m, "timeout_reqs=%lld\n", atomic64_read(&dev->timeout_reqs));
	seq_printf(m, "failed_reqs=%lld\n", atomic64_read(&dev->failed_reqs));
	seq_printf(m, "completed_reqs=%lld\n", atomic64_read(&dev->completed_reqs));
	seq_printf(m, "path_failover_reqs=%lld\n", atomic64_read(&dev->path_failover_reqs));
	seq_printf(m, "probe_failures=%lld\n", atomic64_read(&dev->probe_failures));
	seq_printf(m, "path_state_changes=%lld\n", atomic64_read(&dev->path_state_changes));
	seq_printf(m, "data_inflight_reqs=%d\n", atomic_read(&dev->data_inflight_reqs));
	seq_printf(m, "data_inflight_bytes=%lld\n", atomic64_read(&dev->data_inflight_bytes));
	seq_printf(m, "data_resource_requeued_reqs=%d\n",
		   atomic_read(&dev->data_resource_requeued_reqs));
	seq_printf(m, "data_resource_requeue_events=%lld\n",
		   atomic64_read(&dev->data_resource_requeue_events));
	seq_printf(m, "last_errno=%lld\n", atomic64_read(&dev->last_errno));
	mutex_unlock(&dev->state_lock);
	return 0;
}

static int namrbd_debugfs_paths_show(struct seq_file *m, void *v)
{
	struct namrbd_blk_dev *dev = m->private;
	int i;

	if (!dev || !dev->paths)
		return 0;

	for (i = 0; i < dev->nr_paths; i++) {
		struct namrbd_path *p = &dev->paths[i];

		seq_printf(m,
			   "id=%u gateway_id=%s address=%s port=%u use_tls=%u server_name=%s priority=%u state=%s connected=%u inflight=%d pending=%u pending_high_water=%u outstanding_limit=%u submitted=%llu ewma_ns=%llu completed=%llu retries=%llu conn_opens=%llu conn_resets=%llu consecutive_errors=%u last_errno=%u last_wire_status=%u state_changes=%llu last_transition_jiffies=%lu\n",
			   p->path_id,
			   p->gateway_id[0] ? p->gateway_id : "-",
			   p->endpoint.address[0] ? p->endpoint.address : "-",
			   p->endpoint.port,
			   p->endpoint.use_tls ? 1 : 0,
			   p->endpoint.server_name[0] ? p->endpoint.server_name : "-",
			   p->priority,
			   namrbd_path_state_str(p->state),
			   READ_ONCE(p->sock) ? 1 : 0,
			   atomic_read(&p->inflight),
			   namrbd_path_pending_count(p),
			   p->pending_high_water,
			   p->outstanding_limit,
			   (unsigned long long)p->submitted,
			   (unsigned long long)p->ewma_latency_ns,
			   (unsigned long long)p->completed,
			   (unsigned long long)p->retries,
			   (unsigned long long)p->connection_opens,
			   (unsigned long long)p->connection_resets,
			   p->consecutive_errors, p->last_errno,
			   p->last_wire_status,
			   (unsigned long long)p->state_changes,
			   p->last_transition_jiffies);
	}
	return 0;
}

static int namrbd_debugfs_lanes_show(struct seq_file *m, void *v)
{
	struct namrbd_blk_dev *dev = m->private;
	u32 lane_id;

	if (!dev)
		return 0;

	mutex_lock(&dev->state_lock);
	for (lane_id = 0; lane_id < dev->active_lane_count; lane_id++) {
		u32 path_id = dev->lane_preferred_path_ids[lane_id];
		u32 fallback_path_id = namrbd_lane_fallback_path_id(dev, path_id);
		u32 readiness = namrbd_lane_readiness(dev, path_id, fallback_path_id);

		if (path_id == NAMRBD_PATH_ID_NONE) {
			seq_printf(m, "lane=%u preferred_path_id=none fallback_path_id=none readiness=%s dispatch_reqs=%lld\n",
				   lane_id, namrbd_lane_readiness_str(readiness),
				   atomic64_read(&dev->lane_dispatch_reqs[lane_id]));
		} else if (fallback_path_id == NAMRBD_PATH_ID_NONE) {
			seq_printf(m, "lane=%u preferred_path_id=%u fallback_path_id=none readiness=%s dispatch_reqs=%lld\n",
				   lane_id, path_id, namrbd_lane_readiness_str(readiness),
				   atomic64_read(&dev->lane_dispatch_reqs[lane_id]));
		} else {
			seq_printf(m, "lane=%u preferred_path_id=%u fallback_path_id=%u readiness=%s dispatch_reqs=%lld\n",
				   lane_id, path_id, fallback_path_id,
				   namrbd_lane_readiness_str(readiness),
				   atomic64_read(&dev->lane_dispatch_reqs[lane_id]));
		}
	}
	mutex_unlock(&dev->state_lock);
	return 0;
}

static int namrbd_debugfs_stats_open(struct inode *inode, struct file *file)
{
	return single_open(file, namrbd_debugfs_stats_show, inode->i_private);
}

static int namrbd_debugfs_paths_open(struct inode *inode, struct file *file)
{
	return single_open(file, namrbd_debugfs_paths_show, inode->i_private);
}

static int namrbd_debugfs_lanes_open(struct inode *inode, struct file *file)
{
	return single_open(file, namrbd_debugfs_lanes_show, inode->i_private);
}

static const struct file_operations namrbd_debugfs_stats_fops = {
	.owner = THIS_MODULE,
	.open = namrbd_debugfs_stats_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

static const struct file_operations namrbd_debugfs_paths_fops = {
	.owner = THIS_MODULE,
	.open = namrbd_debugfs_paths_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

static const struct file_operations namrbd_debugfs_lanes_fops = {
	.owner = THIS_MODULE,
	.open = namrbd_debugfs_lanes_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

static void namrbd_debugfs_init(struct namrbd_blk_dev *dev)
{
	char dir_name[16];

	if (!g_mgr.debugfs_devices_root)
		return;

	scnprintf(dir_name, sizeof(dir_name), "%u", dev->device_id);
	dev->debugfs_dir = debugfs_create_dir(dir_name, g_mgr.debugfs_devices_root);
	if (IS_ERR_OR_NULL(dev->debugfs_dir)) {
		dev->debugfs_dir = NULL;
		return;
	}
	debugfs_create_file("stats", 0444, dev->debugfs_dir, dev,
			    &namrbd_debugfs_stats_fops);
	debugfs_create_file("paths", 0444, dev->debugfs_dir, dev,
			    &namrbd_debugfs_paths_fops);
	debugfs_create_file("lanes", 0444, dev->debugfs_dir, dev,
			    &namrbd_debugfs_lanes_fops);
}

static void namrbd_debugfs_cleanup(struct namrbd_blk_dev *dev)
{
	debugfs_remove_recursive(dev->debugfs_dir);
	dev->debugfs_dir = NULL;
}

static int namrbd_probe_data_path(struct namrbd_blk_dev *dev)
{
	u32 limit;
	int i;
	bool lane_changed = false;
	bool any_success = false;
	int first_err = -ENODEV;

	limit = dev->active_path_count ? dev->active_path_count : dev->nr_paths;
	for (i = 0; i < limit; i++) {
		struct namrbd_path *p = &dev->paths[i];
		const struct namrbd_transport_endpoint *endpoint;
		struct socket *sock = NULL;
		u8 header[NAMRBD_WIRE_HDR_LEN];
		u8 resp_hdr[NAMRBD_WIRE_RESP_LEN];
		u8 *resp_payload = NULL;
		u32 resp_len = 0;
		u64 request_id;
		int ret;
		unsigned long flags;

		endpoint = namrbd_transport_endpoint_for_path(p);
		if (!endpoint) {
			ret = -ENODEV;
		} else {
			request_id = atomic64_inc_return(&dev->request_seq);
			namrbd_wire_encode_header(header, NAMRBD_WIRE_OP_PATH_PROBE, 0,
						  request_id,
						  dev->volume_id, dev->generation, 0, 0);
			ret = namrbd_transport_connect(endpoint, &sock);
			if (!ret)
				ret = namrbd_transport_send_all(sock, header, sizeof(header));
			if (!ret)
				ret = namrbd_transport_recv_all(sock, resp_hdr, sizeof(resp_hdr));
			if (!ret)
				ret = namrbd_wire_validate_response_header(
					resp_hdr, namrbd_wire_response_op(NAMRBD_WIRE_OP_PATH_PROBE),
					request_id, dev->volume_id, dev->generation);
			if (!ret) {
				resp_len = get_unaligned_le32(&resp_hdr[48]);
				if (resp_len) {
					resp_payload = kmalloc(resp_len, GFP_KERNEL);
					if (!resp_payload)
						ret = -ENOMEM;
					else
						ret = namrbd_transport_recv_all(sock, resp_payload, resp_len);
				}
				if (!ret)
					ret = namrbd_status_to_errno((s32)get_unaligned_le32(&resp_hdr[56]));
				if (!ret && resp_len >= 32) {
					u32 new_max_io_size = get_unaligned_le32(&resp_payload[0]);
					u32 new_max_zero_like_io_size = resp_len >= 36 ?
						get_unaligned_le32(&resp_payload[32]) : new_max_io_size;

					dev->max_inflight_requests = get_unaligned_le32(&resp_payload[20]);
					dev->max_inflight_bytes = get_unaligned_le64(&resp_payload[24]);
					if (new_max_io_size != dev->max_io_size ||
					    new_max_zero_like_io_size != dev->max_zero_like_io_size)
						ret = namrbd_apply_queue_limits(dev, new_max_io_size,
										new_max_zero_like_io_size,
										"path_probe");
				}
			}
		}
		spin_lock_irqsave(&p->lock, flags);
		if (ret) {
			if (p->state != NAMRBD_PATH_DOWN)
				lane_changed = true;
			p->consecutive_errors = max(p->consecutive_errors,
						    (u32)NAMRBD_PATH_DOWN_THRESHOLD);
			namrbd_path_transition(dev, p, NAMRBD_PATH_DOWN, ret, 0);
		} else {
			p->consecutive_errors = 0;
			if (p->configured_state == NAMRBD_PATH_UP &&
			    p->state != NAMRBD_PATH_UP)
				lane_changed = true;
			if (p->configured_state == NAMRBD_PATH_UP)
				namrbd_path_transition(dev, p, NAMRBD_PATH_UP, 0, 0);
		}
		spin_unlock_irqrestore(&p->lock, flags);
		kfree(resp_payload);
		if (sock)
			sock_release(sock);
		if (ret) {
			if (first_err == -ENODEV)
				first_err = ret;
		} else {
			any_success = true;
		}
	}
	if (lane_changed)
		namrbd_refresh_lane_map(dev, any_success ? "probe_recovered" : "probe_down");
	if (any_success)
		namrbd_wake_no_path_queue(dev, "probe_recovered");
	if (!any_success)
		atomic64_inc(&dev->probe_failures);
	return any_success ? 0 : first_err;
}

static enum namrbd_path_state namrbd_default_path_state(u32 path_id)
{
	if (down_mask & (1UL << path_id))
		return NAMRBD_PATH_DOWN;
	if (draining_mask & (1UL << path_id))
		return NAMRBD_PATH_DRAINING;
	if (degraded_mask & (1UL << path_id))
		return NAMRBD_PATH_DEGRADED;
	return NAMRBD_PATH_UP;
}

static void namrbd_apply_manifest_path_count(struct namrbd_blk_dev *dev, u32 dataplane_path_count)
{
	u32 i;

	if (!dev->paths || dev->nr_paths <= 0)
		return;
	if (!dataplane_path_count)
		dataplane_path_count = 1;
	if (dataplane_path_count > (u32)dev->nr_paths)
		dataplane_path_count = dev->nr_paths;
	dev->active_path_count = dataplane_path_count;

	for (i = 0; i < dev->nr_paths; i++) {
		struct namrbd_path *p = &dev->paths[i];
		enum namrbd_path_state new_state;

		if (i < dataplane_path_count)
			new_state = namrbd_default_path_state(p->path_id);
		else
			new_state = NAMRBD_PATH_DOWN;
		p->configured_state = new_state;
		p->consecutive_errors = 0;
		p->last_errno = 0;
		p->last_wire_status = 0;
		if (p->state != new_state)
			namrbd_path_transition(dev, p, new_state, 0, 0);
	}
	namrbd_refresh_lane_map(dev, "manifest_path_count");
	namrbd_refresh_queue_topology_target_control(dev);
}

static void namrbd_probe_workfn(struct work_struct *work)
{
	struct namrbd_blk_dev *dev = container_of(to_delayed_work(work),
						  struct namrbd_blk_dev, probe_work);

	if (!dev->attached || !dev->paths)
		return;
	/*
	 * Keep probe work focused on datapath liveness only.
	 *
	 * Deferred queue-topology changes can be left in "planned" state after
	 * no-path recovery. Re-applying blk-mq queue-count updates from this
	 * periodic worker is risky because mounted filesystems may still be
	 * draining queued recovery I/O or fsync/journal completions, and
	 * blk_mq_update_nr_hw_queues() can then freeze the queue indefinitely.
	 *
	 * Control-path entry points (activate / configure-data-paths) still own
	 * topology apply decisions; probe only refreshes path health and wakes
	 * queued I/O.
	 */
	namrbd_probe_data_path(dev);
	schedule_delayed_work(&dev->probe_work, HZ);
}

static int namrbd_init_queue(struct namrbd_blk_dev *dev)
{
	int ret;

	memset(&dev->tag_set, 0, sizeof(dev->tag_set));
	dev->tag_set.ops = &namrbd_mq_ops;
	dev->tag_set.nr_hw_queues = 1;
	dev->tag_set.queue_depth = NAMRBD_QUEUE_DEPTH;
	dev->tag_set.numa_node = NUMA_NO_NODE;
	dev->tag_set.cmd_size = 0;
	dev->tag_set.flags = BLK_MQ_F_BLOCKING;
	dev->tag_set.driver_data = dev;

	ret = blk_mq_alloc_tag_set(&dev->tag_set);
	if (ret)
		return ret;

#if NAMRBD_HAVE_BLK_MQ_ALLOC_DISK_3_ARGS
	{
		struct queue_limits lim;

		memset(&lim, 0, sizeof(lim));
		lim.logical_block_size = NAMRBD_BLOCK_SIZE;
		lim.physical_block_size = NAMRBD_BLOCK_SIZE;
		lim.io_min = NAMRBD_BLOCK_SIZE;
		lim.io_opt = NAMRBD_BLOCK_SIZE;
		lim.max_hw_sectors = NAMRBD_DEFAULT_MAX_IO_SECTORS;
		lim.max_sectors = NAMRBD_DEFAULT_MAX_IO_SECTORS;
		namrbd_apply_discard_queue_limits(&lim, NAMRBD_DEFAULT_MAX_IO_SECTORS);

		dev->disk = blk_mq_alloc_disk(&dev->tag_set, &lim, dev);
		if (IS_ERR(dev->disk)) {
			ret = PTR_ERR(dev->disk);
			blk_mq_free_tag_set(&dev->tag_set);
			dev->disk = NULL;
			return ret;
		}
		dev->queue = dev->disk->queue;
	}
#else
	/*
	 * Older blk-mq headers use blk_mq_alloc_disk(set, queuedata) and expose
	 * queue limits through request_queue->limits.
	 */
	dev->disk = blk_mq_alloc_disk(&dev->tag_set, dev);
	if (IS_ERR(dev->disk)) {
		ret = PTR_ERR(dev->disk);
		dev->disk = NULL;
		blk_mq_free_tag_set(&dev->tag_set);
		return ret;
	}
	dev->queue = dev->disk->queue;

	namrbd_set_legacy_queue_limits(dev->queue, NAMRBD_DEFAULT_MAX_IO_SECTORS,
				       NAMRBD_DEFAULT_MAX_IO_SECTORS);
#endif

	return 0;
}

static void namrbd_cleanup_queue(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return;
	blk_mq_free_tag_set(&dev->tag_set);
	dev->queue = NULL;
	dev->disk = NULL;
}

static int namrbd_register_disk(struct namrbd_blk_dev *dev)
{
	int ret;

	if (g_mgr.major <= 0)
		return -ENODEV;

	dev->disk->major = g_mgr.major;
	dev->disk->first_minor = dev->disk_index;
	dev->disk->minors = 1;
	dev->disk->fops = &namrbd_fops;
	dev->disk->private_data = dev;
	strscpy(dev->disk->disk_name, dev->disk_name, DISK_NAME_LEN);
	namrbd_set_disk_capacity(dev->disk, 0, false);
	ret = add_disk(dev->disk);
	if (ret)
		return ret;

	return 0;
}

static void namrbd_unregister_disk(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return;

	if (dev->disk) {
		del_gendisk(dev->disk);
		put_disk(dev->disk);
		dev->disk = NULL;
		dev->queue = NULL;
	}
}

static int namrbd_init_device_defaults(struct namrbd_blk_dev *dev)
{
	int ret;

	dev->size_bytes = size_mb * 1024ULL * 1024ULL;
	if (dev->size_bytes < NAMRBD_BLOCK_SIZE)
		return -EINVAL;

	dev->attached = false;
	dev->volume_id = 0;
	dev->generation = 0;
	dev->attached_host_id[0] = '\0';
	dev->dataplane_auth_mode[0] = '\0';
	dev->dataplane_token[0] = '\0';
	dev->fail_path_id = fail_path_id;
	spin_lock_init(&dev->data_lock);
	spin_lock_init(&dev->zero_map_lock);
	mutex_init(&dev->state_lock);
	dev->max_io_size = NAMRBD_DEFAULT_MAX_IO_SIZE;
	dev->max_data_io_size = NAMRBD_DEFAULT_MAX_DATA_IO_SIZE;
	dev->max_zero_like_io_size = NAMRBD_DEFAULT_MAX_IO_SIZE;
	dev->max_inflight_requests = NAMRBD_DEFAULT_MAX_INFLIGHT_REQS;
	dev->max_inflight_bytes = NAMRBD_DEFAULT_MAX_INFLIGHT_BYTES;
	dev->target_nr_hw_queues = 1;
	dev->queue_topology_generation = 0;
	strscpy(dev->queue_topology_state, "stable", sizeof(dev->queue_topology_state));
	ret = namrbd_parse_no_path_retry(no_path_retry, &dev->no_path_retry_mode,
					 &dev->no_path_retry_seconds);
	if (ret)
		return ret;
	dev->no_path_state = NAMRBD_NO_PATH_INACTIVE;
	dev->no_path_since_jiffies = 0;
	dev->no_path_retry_deadline_jiffies = 0;
	dev->last_no_path_wakeup_jiffies = 0;
	atomic_set(&dev->data_inflight_reqs, 0);
	atomic64_set(&dev->data_inflight_bytes, 0);
	atomic_set(&dev->data_resource_requeued_reqs, 0);
	atomic64_set(&dev->data_resource_requeue_events, 0);
	spin_lock_init(&dev->data_resource_requeue_lock);
	INIT_LIST_HEAD(&dev->data_resource_requeue_list);
	atomic64_set(&dev->request_seq, 0);
	atomic64_set(&dev->no_path_reqs, 0);
	atomic64_set(&dev->no_path_queued_reqs, 0);
	atomic64_set(&dev->no_path_requeued_reqs, 0);
	atomic64_set(&dev->no_path_failed_reqs, 0);
	atomic64_set(&dev->no_path_recovered_reqs, 0);
	atomic64_set(&dev->no_path_enter_count, 0);
	INIT_DELAYED_WORK(&dev->probe_work, namrbd_probe_workfn);

	dev->data = vmalloc(dev->size_bytes);
	if (!dev->data)
		return -ENOMEM;
	memset(dev->data, 0, dev->size_bytes);

	return namrbd_init_paths(dev);
}

static void namrbd_cleanup_device_memory(struct namrbd_blk_dev *dev)
{
	cancel_delayed_work_sync(&dev->probe_work);
	namrbd_cleanup_resource_requeues(dev);
	namrbd_cleanup_paths(dev);
	namrbd_zero_map_free(dev);
	vfree(dev->data);
	dev->data = NULL;
}

static void namrbd_free_device(struct namrbd_blk_dev *dev)
{
	if (!dev)
		return;
	namrbd_debugfs_cleanup(dev);
	namrbd_sysfs_remove(dev);
	namrbd_unregister_disk(dev);
	namrbd_cleanup_queue(dev);
	namrbd_cleanup_device_memory(dev);
	kfree(dev);
}

int namrbd_blk_create(u32 *device_id_out)
{
	struct namrbd_blk_dev *dev;
	int ret;
	int device_id;
	int disk_index;

	if (!device_id_out)
		return -EINVAL;

	dev = kzalloc(sizeof(*dev), GFP_KERNEL);
	if (!dev)
		return -ENOMEM;

	ret = namrbd_init_device_defaults(dev);
	if (ret)
		goto out_free_dev;

	ret = namrbd_init_queue(dev);
	if (ret)
		goto out_cleanup_memory;

	mutex_lock(&g_mgr.lock);
	disk_index = ida_alloc(&g_mgr.disk_indexes, GFP_KERNEL);
	if (disk_index < 0) {
		ret = disk_index;
		mutex_unlock(&g_mgr.lock);
		goto out_cleanup_queue;
	}

	device_id = idr_alloc(&g_mgr.devices, dev, 0, 0, GFP_KERNEL);
	if (device_id < 0) {
		ret = device_id;
		ida_free(&g_mgr.disk_indexes, disk_index);
		mutex_unlock(&g_mgr.lock);
		goto out_cleanup_queue;
	}

	dev->device_id = device_id;
	dev->disk_index = disk_index;
	scnprintf(dev->disk_name, sizeof(dev->disk_name), "%s%u",
		  NAMRBD_DISK_NAME_PREFIX, dev->disk_index);
	list_add_tail(&dev->list, &g_mgr.device_list);
	mutex_unlock(&g_mgr.lock);

	ret = namrbd_register_disk(dev);
	if (ret)
		goto out_remove_ids;

	ret = namrbd_sysfs_create(dev);
	if (ret)
		goto out_unregister_disk;

	namrbd_debugfs_init(dev);
	*device_id_out = dev->device_id;

	pr_info("namrbd_blk: created device_id=%u disk=%s size=%llu MiB paths=%d policy=%s\n",
		dev->device_id, dev->disk_name, size_mb, dev->nr_paths, sched_policy);
	return 0;

out_unregister_disk:
	namrbd_unregister_disk(dev);
out_remove_ids:
	mutex_lock(&g_mgr.lock);
	list_del(&dev->list);
	idr_remove(&g_mgr.devices, dev->device_id);
	ida_free(&g_mgr.disk_indexes, dev->disk_index);
	mutex_unlock(&g_mgr.lock);
out_cleanup_queue:
	namrbd_cleanup_queue(dev);
out_cleanup_memory:
	namrbd_cleanup_device_memory(dev);
out_free_dev:
	kfree(dev);
	return ret;
}
EXPORT_SYMBOL(namrbd_blk_create);

int namrbd_blk_destroy(u32 device_id)
{
	struct namrbd_blk_dev *dev;

	mutex_lock(&g_mgr.lock);
	dev = namrbd_blk_lookup_device_locked(device_id);
	if (!dev) {
		mutex_unlock(&g_mgr.lock);
		return -ENODEV;
	}
	mutex_lock(&dev->state_lock);
	if (dev->attached) {
		mutex_unlock(&dev->state_lock);
		mutex_unlock(&g_mgr.lock);
		return -EBUSY;
	}
	list_del(&dev->list);
	idr_remove(&g_mgr.devices, dev->device_id);
	ida_free(&g_mgr.disk_indexes, dev->disk_index);
	mutex_unlock(&dev->state_lock);
	mutex_unlock(&g_mgr.lock);

	namrbd_free_device(dev);
	pr_info("namrbd_blk: destroyed device_id=%u\n", device_id);
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_destroy);

int namrbd_blk_activate_device_with_initial_zero_map(u32 device_id, u64 volume_id,
						     u64 size_bytes, u32 block_size,
						     u32 chunk_size_bytes, u64 generation,
						     bool initial_zero_map_all_zero)
{
	struct namrbd_blk_dev *dev;
	u8 *new_data;
	u8 *old_data;
	unsigned long flags;
	bool remote_backed;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev || !dev->disk || !dev->queue)
		return -ENODEV;
	if (block_size != NAMRBD_BLOCK_SIZE)
		return -EINVAL;
	if (!chunk_size_bytes || chunk_size_bytes < block_size ||
	    (chunk_size_bytes % block_size) != 0)
		return -EINVAL;
	if (size_bytes < NAMRBD_BLOCK_SIZE)
		return -EINVAL;

	remote_backed = namrbd_device_has_dataplane_endpoint(dev);
	if (remote_backed) {
		new_data = NULL;
	} else {
		new_data = vmalloc(size_bytes);
		if (!new_data)
			return -ENOMEM;
		memset(new_data, 0, size_bytes);
	}

	mutex_lock(&dev->state_lock);
	blk_mq_quiesce_queue(dev->queue);

	spin_lock_irqsave(&dev->data_lock, flags);
	old_data = dev->data;
	dev->data = new_data;
	dev->size_bytes = size_bytes;
	dev->volume_id = volume_id;
	dev->generation = generation;
	dev->chunk_size_bytes = chunk_size_bytes;
	dev->attached = true;
	spin_unlock_irqrestore(&dev->data_lock, flags);
	vfree(old_data);
	if (remote_backed) {
		if (namrbd_zero_map_init(dev, size_bytes, initial_zero_map_all_zero)) {
			namrbd_zero_map_free(dev);
			pr_warn("namrbd_blk: device_id=%u zero_map allocation failed; continuing without local zero-like skips\n",
				device_id);
		}
	} else {
		namrbd_zero_map_free(dev);
	}

	namrbd_set_disk_capacity(dev->disk, dev->size_bytes / NAMRBD_SECTOR_SIZE, false);
	blk_mq_unquiesce_queue(dev->queue);
	mutex_unlock(&dev->state_lock);
	namrbd_apply_queue_topology_control(dev, "activate");
	if (remote_backed)
		schedule_delayed_work(&dev->probe_work, 0);

	pr_info("namrbd_blk: activated device_id=%u volume=%08x generation=%llu size_bytes=%llu backing=%s\n",
		device_id, (u32)volume_id,
		(unsigned long long)generation, (unsigned long long)size_bytes,
		remote_backed ? "dataplane" : "memory");
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_activate_device_with_initial_zero_map);

int namrbd_blk_activate_device(u32 device_id, u64 volume_id, u64 size_bytes,
			       u32 block_size, u32 chunk_size_bytes, u64 generation)
{
	return namrbd_blk_activate_device_with_initial_zero_map(device_id, volume_id,
							       size_bytes, block_size,
							       chunk_size_bytes, generation,
							       false);
}
EXPORT_SYMBOL(namrbd_blk_activate_device);

int namrbd_blk_resize_device(u32 device_id, u64 volume_id, u64 generation, u64 size_bytes)
{
	struct namrbd_blk_dev *dev;
	u8 *new_data;
	u8 *old_data;
	u64 old_size_bytes;
	unsigned long flags;
	bool remote_backed;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev || !dev->disk || !dev->queue)
		return -ENODEV;
	if (size_bytes < NAMRBD_BLOCK_SIZE || (size_bytes % NAMRBD_BLOCK_SIZE) != 0)
		return -EINVAL;

	mutex_lock(&dev->state_lock);
	if (!dev->attached) {
		mutex_unlock(&dev->state_lock);
		return -ENODEV;
	}
	if (dev->volume_id != volume_id || dev->generation != generation) {
		mutex_unlock(&dev->state_lock);
		return -ESTALE;
	}
	old_size_bytes = dev->size_bytes;
	if (size_bytes < old_size_bytes) {
		mutex_unlock(&dev->state_lock);
		return -EINVAL;
	}
	if (size_bytes == old_size_bytes) {
		mutex_unlock(&dev->state_lock);
		return 0;
	}

	remote_backed = namrbd_device_has_dataplane_endpoint(dev);
	if (remote_backed) {
		new_data = NULL;
	} else {
		new_data = vmalloc(size_bytes);
		if (!new_data) {
			mutex_unlock(&dev->state_lock);
			return -ENOMEM;
		}
		memset(new_data, 0, size_bytes);
	}

	blk_mq_quiesce_queue(dev->queue);
	old_data = dev->data;
	if (!remote_backed && old_data && old_size_bytes)
		memcpy(new_data, old_data, old_size_bytes);
	spin_lock_irqsave(&dev->data_lock, flags);
	dev->data = new_data;
	dev->size_bytes = size_bytes;
	spin_unlock_irqrestore(&dev->data_lock, flags);
	vfree(old_data);
	namrbd_zero_map_free(dev);

	namrbd_set_disk_capacity(dev->disk, dev->size_bytes / NAMRBD_SECTOR_SIZE, true);
	blk_mq_unquiesce_queue(dev->queue);
	mutex_unlock(&dev->state_lock);

	pr_info("namrbd_blk: resized device_id=%u volume=%08x generation=%llu old_size_bytes=%llu size_bytes=%llu backing=%s\n",
		device_id, (u32)volume_id, (unsigned long long)generation,
		(unsigned long long)old_size_bytes, (unsigned long long)size_bytes,
		remote_backed ? "dataplane" : "memory");
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_resize_device);

int namrbd_blk_configure_data_paths_device(u32 device_id,
					   const struct namrbd_transport_path *paths,
					   u32 dataplane_path_count,
					   u32 max_inflight_requests,
					   u64 max_inflight_bytes,
					   u32 max_io_size,
					   u32 max_zero_like_io_size,
					   const char *host_id,
					   const char *dataplane_auth_mode,
					   const char *dataplane_token,
					   const char *dataplane_session_key)
{
	struct namrbd_blk_dev *dev;
	u32 i;
	int ret;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev)
		return -EINVAL;
	ret = namrbd_validate_transport_paths(paths, dataplane_path_count,
						      (u32)NAMRBD_MAX_PATHS);
	if (ret)
		return ret;
	if (dataplane_path_count > (u32)dev->nr_paths)
		dataplane_path_count = (u32)dev->nr_paths;

	mutex_lock(&dev->state_lock);
	for (i = 0; i < (u32)dev->nr_paths; i++) {
		struct namrbd_path *path = &dev->paths[i];

		namrbd_path_close_socket(path);
		path->endpoint.port = 0;
		path->endpoint.address[0] = '\0';
		path->endpoint.server_name[0] = '\0';
		path->endpoint.use_tls = false;
		path->gateway_id[0] = '\0';
		path->priority = 0;
		if (i < dataplane_path_count) {
			path->path_id = paths[i].path_id;
			path->priority = paths[i].priority;
			strscpy(path->gateway_id, paths[i].gateway_id, sizeof(path->gateway_id));
			memcpy(&path->endpoint, &paths[i].endpoint, sizeof(path->endpoint));
		} else {
			path->path_id = i;
		}
	}
	dev->max_inflight_requests = max_inflight_requests ? max_inflight_requests :
					 NAMRBD_DEFAULT_MAX_INFLIGHT_REQS;
	dev->max_inflight_bytes = max_inflight_bytes ? max_inflight_bytes :
				      NAMRBD_DEFAULT_MAX_INFLIGHT_BYTES;
	if (host_id && host_id[0])
		strscpy(dev->attached_host_id, host_id, sizeof(dev->attached_host_id));
	else
		dev->attached_host_id[0] = '\0';
	ret = namrbd_apply_queue_limits(dev,
					max_io_size ? max_io_size :
						      NAMRBD_DEFAULT_MAX_IO_SIZE,
					max_zero_like_io_size ? max_zero_like_io_size :
								max_io_size,
					"configure_data_paths");
	if (ret) {
		mutex_unlock(&dev->state_lock);
		return ret;
	}
	if (dataplane_auth_mode && dataplane_auth_mode[0])
		strscpy(dev->dataplane_auth_mode, dataplane_auth_mode,
			sizeof(dev->dataplane_auth_mode));
	else
		dev->dataplane_auth_mode[0] = '\0';
	if (dataplane_token && dataplane_token[0])
		strscpy(dev->dataplane_token, dataplane_token, sizeof(dev->dataplane_token));
	else
		dev->dataplane_token[0] = '\0';
	if (dataplane_session_key && dataplane_session_key[0])
		strscpy(dev->dataplane_session_key, dataplane_session_key,
			sizeof(dev->dataplane_session_key));
	else
		dev->dataplane_session_key[0] = '\0';
	namrbd_apply_manifest_path_count(dev, dataplane_path_count);
	mutex_unlock(&dev->state_lock);
	namrbd_apply_queue_topology_control(dev, "configure_data_paths");

	cancel_delayed_work_sync(&dev->probe_work);
	schedule_delayed_work(&dev->probe_work, 0);
	if (namrbd_classify_no_path(dev, REQ_OP_READ, 0, NULL) == NAMRBD_NO_PATH_NONE)
		namrbd_wake_no_path_queue(dev, "configure_data_paths");
	pr_info("namrbd_blk: device_id=%u datapaths=%u active_paths=%u max_io=%u max_data_io=%u max_zero_like_io=%u inflight_reqs=%u inflight_bytes=%llu\n",
		device_id, dataplane_path_count, dataplane_path_count ? dataplane_path_count : 1,
		dev->max_io_size,
		dev->max_data_io_size,
		dev->max_zero_like_io_size,
		dev->max_inflight_requests,
		(unsigned long long)dev->max_inflight_bytes);
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_configure_data_paths_device);

int namrbd_blk_configure_data_path_device(u32 device_id, const char *address, u16 port,
					  u32 dataplane_path_count,
					  u32 max_inflight_requests,
					  u64 max_inflight_bytes,
					  u32 max_io_size,
					  u32 max_zero_like_io_size,
					  const char *host_id,
					  const char *dataplane_auth_mode,
					  const char *dataplane_token,
					  const char *dataplane_session_key)
{
	struct namrbd_transport_path *paths;
	u32 i;
	int ret;

	if (!address || !address[0] || !port)
		return -EINVAL;
	if (!dataplane_path_count)
		dataplane_path_count = 1;
	if (dataplane_path_count > NAMRBD_TRANSPORT_MAX_PATHS)
		dataplane_path_count = NAMRBD_TRANSPORT_MAX_PATHS;
	paths = kcalloc(dataplane_path_count, sizeof(*paths), GFP_KERNEL);
	if (!paths)
		return -ENOMEM;
	for (i = 0; i < dataplane_path_count; i++) {
		paths[i].path_id = i;
		paths[i].endpoint.port = port;
		strscpy(paths[i].endpoint.address, address, sizeof(paths[i].endpoint.address));
	}
	ret = namrbd_blk_configure_data_paths_device(device_id, paths, dataplane_path_count,
						     max_inflight_requests,
						     max_inflight_bytes,
						     max_io_size,
						     max_zero_like_io_size,
						     host_id,
						     dataplane_auth_mode,
						     dataplane_token,
						     dataplane_session_key);
	kfree(paths);
	return ret;
}
EXPORT_SYMBOL(namrbd_blk_configure_data_path_device);

int namrbd_blk_get_status(u32 device_id, struct namrbd_blk_status *out)
{
	struct namrbd_blk_dev *dev;
	u32 i;

	if (!out)
		return -EINVAL;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev)
		return -ENODEV;

	mutex_lock(&dev->state_lock);
	memset(out, 0, sizeof(*out));
	out->device_id = dev->device_id;
	strscpy(out->disk_name, dev->disk_name, sizeof(out->disk_name));
	out->attached = dev->attached ? 1 : 0;
	out->volume_id = dev->attached ? dev->volume_id : 0;
	out->generation = dev->generation;
	out->applied_path_plan_revision = dev->applied_path_plan_revision;
	out->path_count = dev->active_path_count ? dev->active_path_count : dev->nr_paths;
	out->active_lane_count = dev->active_lane_count;
	out->nr_hw_queues = dev->tag_set.nr_hw_queues;
	out->target_nr_hw_queues = dev->target_nr_hw_queues;
	out->queue_topology_generation = dev->queue_topology_generation;
	strscpy(out->queue_topology_state, dev->queue_topology_state,
		sizeof(out->queue_topology_state));
	out->lane_remap_count = dev->lane_remap_count;
	out->last_lane_remapped_lanes = dev->last_lane_remapped_lanes;
	out->last_lane_remap_jiffies = dev->last_lane_remap_jiffies;
	strscpy(out->last_lane_remap_reason, dev->last_lane_remap_reason,
		sizeof(out->last_lane_remap_reason));
	out->no_path_retry_mode = (u32)dev->no_path_retry_mode;
	out->no_path_retry_seconds = dev->no_path_retry_seconds;
	out->no_path_state = (u32)dev->no_path_state;
	out->no_path_since_jiffies = dev->no_path_since_jiffies;
	out->no_path_retry_deadline_jiffies = dev->no_path_retry_deadline_jiffies;
	out->last_no_path_wakeup_jiffies = dev->last_no_path_wakeup_jiffies;
	out->no_path_queued_reqs = atomic64_read(&dev->no_path_queued_reqs);
	out->no_path_requeued_reqs = atomic64_read(&dev->no_path_requeued_reqs);
	out->no_path_failed_reqs = atomic64_read(&dev->no_path_failed_reqs);
	out->no_path_recovered_reqs = atomic64_read(&dev->no_path_recovered_reqs);
	out->no_path_enter_count = atomic64_read(&dev->no_path_enter_count);
	out->last_no_path_reason = dev->last_no_path_reason;
	out->last_no_path_op = dev->last_no_path_op;
	out->last_no_path_eligible_paths = dev->last_no_path_eligible_paths;
	out->last_no_path_tried_mask = dev->last_no_path_tried_mask;
	out->last_no_path_jiffies = dev->last_no_path_jiffies;
	for (i = 0; i < out->path_count; i++) {
		struct namrbd_path *p = &dev->paths[i];
		out->paths[i].path_id = p->path_id;
		out->paths[i].state = (u32)p->state;
		out->paths[i].consecutive_errors = p->consecutive_errors;
		out->paths[i].last_errno = p->last_errno;
		out->paths[i].last_wire_status = p->last_wire_status;
		out->paths[i].priority = p->priority;
		out->paths[i].connected = READ_ONCE(p->sock) ? 1 : 0;
		out->paths[i].inflight = (u32)atomic_read(&p->inflight);
		out->paths[i].pending = namrbd_path_pending_count(p);
		out->paths[i].pending_high_water = p->pending_high_water;
		out->paths[i].outstanding_limit = p->outstanding_limit;
		spin_lock(&p->lock);
		out->paths[i].submitted = p->submitted;
		out->paths[i].completed = p->completed;
		out->paths[i].retries = p->retries;
		out->paths[i].conn_opens = p->connection_opens;
		out->paths[i].conn_resets = p->connection_resets;
		spin_unlock(&p->lock);
		out->paths[i].port = p->endpoint.port;
		out->paths[i].use_tls = p->endpoint.use_tls ? 1 : 0;
		strscpy(out->paths[i].gateway_id, p->gateway_id, sizeof(out->paths[i].gateway_id));
		strscpy(out->paths[i].address, p->endpoint.address, sizeof(out->paths[i].address));
		strscpy(out->paths[i].server_name, p->endpoint.server_name,
			sizeof(out->paths[i].server_name));

		switch (p->state) {
		case NAMRBD_PATH_DOWN:
			out->down_mask |= (1ULL << i);
			break;
		case NAMRBD_PATH_DEGRADED:
			out->degraded_mask |= (1ULL << i);
			break;
		case NAMRBD_PATH_DRAINING:
			out->draining_mask |= (1ULL << i);
			break;
		default:
			break;
		}
	}
	for (i = 0; i < out->active_lane_count; i++) {
		u32 preferred_path_id = dev->lane_preferred_path_ids[i];
		u32 fallback_path_id =
			namrbd_lane_fallback_path_id(dev, preferred_path_id);
		out->lanes[i].lane_id = i;
		out->lanes[i].preferred_path_id = preferred_path_id;
		out->lanes[i].fallback_path_id = fallback_path_id;
		out->lanes[i].readiness =
			namrbd_lane_readiness(dev, preferred_path_id, fallback_path_id);
		out->lanes[i].dispatch_reqs =
			atomic64_read(&dev->lane_dispatch_reqs[i]);
	}
	mutex_unlock(&dev->state_lock);
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_get_status);

int namrbd_blk_update_path_masks_device(u32 device_id, u64 path_plan_revision,
					u64 down_mask_bits, u64 degraded_mask_bits, u64 draining_mask_bits)
{
	struct namrbd_blk_dev *dev;
	u32 i;
	int ret = 0;
	u64 prev_applied_revision;
	u64 current_down_mask = 0;
	u64 current_degraded_mask = 0;
	u64 current_draining_mask = 0;
	bool same_masks;
	bool idempotent_retry = false;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev)
		return -ENODEV;

	mutex_lock(&dev->state_lock);
	prev_applied_revision = dev->applied_path_plan_revision;
	for (i = 0; i < dev->nr_paths; i++) {
		switch (dev->paths[i].configured_state) {
		case NAMRBD_PATH_DOWN:
			current_down_mask |= (1ULL << i);
			break;
		case NAMRBD_PATH_DEGRADED:
			current_degraded_mask |= (1ULL << i);
			break;
		case NAMRBD_PATH_DRAINING:
			current_draining_mask |= (1ULL << i);
			break;
		case NAMRBD_PATH_UP:
		default:
			break;
		}
	}
	same_masks = down_mask_bits == current_down_mask &&
		degraded_mask_bits == current_degraded_mask &&
		draining_mask_bits == current_draining_mask;
	if (path_plan_revision == 0) {
		/*
		 * Legacy callers may omit revisions, but once a versioned plan has
		 * been applied we should not let an unversioned update overwrite it.
		 */
		if (dev->applied_path_plan_revision != 0) {
			ret = -ESTALE;
			goto unlock;
		}
		if (same_masks) {
			idempotent_retry = true;
			goto unlock;
		}
	} else if (path_plan_revision < prev_applied_revision) {
		ret = -ESTALE;
		goto unlock;
	} else if (path_plan_revision == prev_applied_revision) {
		if (same_masks) {
			/* Idempotent retry: same revision and masks are a no-op. */
			idempotent_retry = true;
		} else {
			ret = -ESTALE;
		}
		goto unlock;
	}
	for (i = 0; i < dev->nr_paths; i++) {
		struct namrbd_path *p = &dev->paths[i];
		enum namrbd_path_state new_state = NAMRBD_PATH_UP;

		if (down_mask_bits & (1ULL << i))
			new_state = NAMRBD_PATH_DOWN;
		else if (draining_mask_bits & (1ULL << i))
			new_state = NAMRBD_PATH_DRAINING;
		else if (degraded_mask_bits & (1ULL << i))
			new_state = NAMRBD_PATH_DEGRADED;

		p->configured_state = new_state;
		p->consecutive_errors = 0;
		p->last_errno = 0;
		p->last_wire_status = 0;
		if (new_state == NAMRBD_PATH_DOWN || new_state == NAMRBD_PATH_DRAINING)
			namrbd_path_close_socket(p);
		if (p->state != new_state)
			namrbd_path_transition(dev, p, new_state, 0, 0);
	}
	dev->applied_path_plan_revision = path_plan_revision;
	namrbd_refresh_lane_map(dev, "path_plan_apply");
unlock:
	mutex_unlock(&dev->state_lock);
	if (ret == -ESTALE) {
		pr_warn("namrbd_blk: device_id=%u rejected stale path-plan revision=%llu applied=%llu\n",
			device_id,
			(unsigned long long)path_plan_revision,
			(unsigned long long)prev_applied_revision);
		return ret;
	}
	if (idempotent_retry &&
	    down_mask_bits == 0 && degraded_mask_bits == 0 && draining_mask_bits == 0) {
		/*
		 * Keep the fast path quiet for empty-idempotent retries that can
		 * happen during early bootstrapping.
		 */
		return 0;
	}
	if (idempotent_retry) {
		pr_info("namrbd_blk: device_id=%u skipped idempotent path-plan revision=%llu\n",
			device_id, (unsigned long long)path_plan_revision);
		return 0;
	}

	pr_info("namrbd_blk: device_id=%u updated path masks revision=%llu down=0x%llx degraded=0x%llx draining=0x%llx\n",
		device_id,
		(unsigned long long)path_plan_revision,
		(unsigned long long)down_mask_bits,
		(unsigned long long)degraded_mask_bits,
		(unsigned long long)draining_mask_bits);
	if (namrbd_classify_no_path(dev, REQ_OP_READ, 0, NULL) == NAMRBD_NO_PATH_NONE)
		namrbd_wake_no_path_queue(dev, "path_plan_apply");
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_update_path_masks_device);

int namrbd_blk_list_devices(struct namrbd_blk_status *out, u32 max_entries, u32 *count_out)
{
	struct namrbd_blk_dev *dev;
	u32 count = 0;

	if (!out || !count_out)
		return -EINVAL;

	mutex_lock(&g_mgr.lock);
	list_for_each_entry(dev, &g_mgr.device_list, list) {
		if (count >= max_entries) {
			mutex_unlock(&g_mgr.lock);
			return -ENOSPC;
		}

		mutex_lock(&dev->state_lock);
		memset(&out[count], 0, sizeof(out[count]));
		out[count].device_id = dev->device_id;
		strscpy(out[count].disk_name, dev->disk_name, sizeof(out[count].disk_name));
		out[count].attached = dev->attached ? 1 : 0;
		out[count].volume_id = dev->attached ? dev->volume_id : 0;
		out[count].generation = dev->generation;
		out[count].applied_path_plan_revision = dev->applied_path_plan_revision;
		out[count].path_count = dev->active_path_count ? dev->active_path_count : dev->nr_paths;
		out[count].active_lane_count = dev->active_lane_count;
		out[count].nr_hw_queues = dev->tag_set.nr_hw_queues;
		out[count].target_nr_hw_queues = dev->target_nr_hw_queues;
		out[count].queue_topology_generation = dev->queue_topology_generation;
		strscpy(out[count].queue_topology_state, dev->queue_topology_state,
			sizeof(out[count].queue_topology_state));
		out[count].lane_remap_count = dev->lane_remap_count;
		out[count].last_lane_remapped_lanes = dev->last_lane_remapped_lanes;
		out[count].last_lane_remap_jiffies = dev->last_lane_remap_jiffies;
		strscpy(out[count].last_lane_remap_reason, dev->last_lane_remap_reason,
			sizeof(out[count].last_lane_remap_reason));
		out[count].no_path_retry_mode = (u32)dev->no_path_retry_mode;
		out[count].no_path_retry_seconds = dev->no_path_retry_seconds;
		out[count].no_path_state = (u32)dev->no_path_state;
		out[count].no_path_since_jiffies = dev->no_path_since_jiffies;
		out[count].no_path_retry_deadline_jiffies =
			dev->no_path_retry_deadline_jiffies;
		out[count].last_no_path_wakeup_jiffies =
			dev->last_no_path_wakeup_jiffies;
		out[count].no_path_queued_reqs =
			atomic64_read(&dev->no_path_queued_reqs);
		out[count].no_path_requeued_reqs =
			atomic64_read(&dev->no_path_requeued_reqs);
		out[count].no_path_failed_reqs =
			atomic64_read(&dev->no_path_failed_reqs);
		out[count].no_path_recovered_reqs =
			atomic64_read(&dev->no_path_recovered_reqs);
		out[count].no_path_enter_count =
			atomic64_read(&dev->no_path_enter_count);
		out[count].last_no_path_reason = dev->last_no_path_reason;
		out[count].last_no_path_op = dev->last_no_path_op;
		out[count].last_no_path_eligible_paths =
			dev->last_no_path_eligible_paths;
		out[count].last_no_path_tried_mask = dev->last_no_path_tried_mask;
		out[count].last_no_path_jiffies = dev->last_no_path_jiffies;
		if (dev->paths) {
			u32 i;

			for (i = 0; i < out[count].path_count; i++) {
				out[count].paths[i].path_id = dev->paths[i].path_id;
				out[count].paths[i].state = (u32)dev->paths[i].state;
				out[count].paths[i].consecutive_errors = dev->paths[i].consecutive_errors;
				out[count].paths[i].last_errno = dev->paths[i].last_errno;
				out[count].paths[i].last_wire_status = dev->paths[i].last_wire_status;
				out[count].paths[i].priority = dev->paths[i].priority;
				out[count].paths[i].connected =
					READ_ONCE(dev->paths[i].sock) ? 1 : 0;
				out[count].paths[i].inflight =
					(u32)atomic_read(&dev->paths[i].inflight);
				out[count].paths[i].pending =
					namrbd_path_pending_count(&dev->paths[i]);
				out[count].paths[i].pending_high_water =
					dev->paths[i].pending_high_water;
				out[count].paths[i].outstanding_limit =
					dev->paths[i].outstanding_limit;
				spin_lock(&dev->paths[i].lock);
				out[count].paths[i].submitted = dev->paths[i].submitted;
				out[count].paths[i].completed = dev->paths[i].completed;
				out[count].paths[i].retries = dev->paths[i].retries;
				out[count].paths[i].conn_opens = dev->paths[i].connection_opens;
				out[count].paths[i].conn_resets = dev->paths[i].connection_resets;
				spin_unlock(&dev->paths[i].lock);
				out[count].paths[i].port = dev->paths[i].endpoint.port;
				out[count].paths[i].use_tls = dev->paths[i].endpoint.use_tls ? 1 : 0;
				strscpy(out[count].paths[i].gateway_id, dev->paths[i].gateway_id,
					sizeof(out[count].paths[i].gateway_id));
				strscpy(out[count].paths[i].address, dev->paths[i].endpoint.address,
					sizeof(out[count].paths[i].address));
				strscpy(out[count].paths[i].server_name,
					dev->paths[i].endpoint.server_name,
					sizeof(out[count].paths[i].server_name));
				switch (dev->paths[i].state) {
				case NAMRBD_PATH_DOWN:
					out[count].down_mask |= (1ULL << i);
					break;
				case NAMRBD_PATH_DEGRADED:
					out[count].degraded_mask |= (1ULL << i);
					break;
				case NAMRBD_PATH_DRAINING:
					out[count].draining_mask |= (1ULL << i);
					break;
				default:
					break;
				}
			}
		}
		{
			u32 i;

			for (i = 0; i < out[count].active_lane_count; i++) {
				u32 preferred_path_id = dev->lane_preferred_path_ids[i];
				u32 fallback_path_id =
					namrbd_lane_fallback_path_id(dev, preferred_path_id);
				out[count].lanes[i].lane_id = i;
				out[count].lanes[i].preferred_path_id = preferred_path_id;
				out[count].lanes[i].fallback_path_id = fallback_path_id;
				out[count].lanes[i].readiness =
					namrbd_lane_readiness(dev, preferred_path_id, fallback_path_id);
				out[count].lanes[i].dispatch_reqs =
					atomic64_read(&dev->lane_dispatch_reqs[i]);
			}
		}
		mutex_unlock(&dev->state_lock);
		count++;
	}
	mutex_unlock(&g_mgr.lock);

	*count_out = count;
	return 0;
}
EXPORT_SYMBOL(namrbd_blk_list_devices);

void namrbd_blk_deactivate_device(u32 device_id, u64 volume_id)
{
	struct namrbd_blk_dev *dev;
	u32 i;

	dev = namrbd_blk_lookup_device(device_id);
	if (!dev || !dev->disk || !dev->queue)
		return;

	mutex_lock(&dev->state_lock);
	blk_mq_quiesce_queue(dev->queue);
	if (dev->attached && (!volume_id || dev->volume_id == volume_id)) {
		dev->attached = false;
		dev->generation = 0;
		dev->volume_id = 0;
		dev->active_path_count = dev->nr_paths;
		dev->attached_host_id[0] = '\0';
		dev->dataplane_auth_mode[0] = '\0';
		dev->dataplane_token[0] = '\0';
		dev->dataplane_session_key[0] = '\0';
		namrbd_zero_map_free(dev);
		for (i = 0; i < dev->nr_paths; i++) {
			namrbd_path_close_socket(&dev->paths[i]);
			dev->paths[i].endpoint.port = 0;
			dev->paths[i].endpoint.address[0] = '\0';
			dev->paths[i].endpoint.server_name[0] = '\0';
			dev->paths[i].endpoint.use_tls = false;
			dev->paths[i].gateway_id[0] = '\0';
			dev->paths[i].priority = 0;
			dev->paths[i].path_id = i;
		}
		namrbd_set_disk_capacity(dev->disk, 0, true);
	}
	blk_mq_unquiesce_queue(dev->queue);
	namrbd_kick_no_path_queue(dev, "deactivate");
	mutex_unlock(&dev->state_lock);
	cancel_delayed_work_sync(&dev->probe_work);

	pr_info("namrbd_blk: deactivated device_id=%u volume=%08x\n",
		device_id, (u32)volume_id);
}
EXPORT_SYMBOL(namrbd_blk_deactivate_device);

int namrbd_blk_activate(u64 volume_id, u64 size_bytes, u32 block_size,
			u32 chunk_size_bytes, u64 generation)
{
	struct namrbd_blk_dev *dev = namrbd_blk_lookup_default_device();

	if (!dev)
		return -ENODEV;
	return namrbd_blk_activate_device(dev->device_id, volume_id, size_bytes,
					  block_size, chunk_size_bytes, generation);
}
EXPORT_SYMBOL(namrbd_blk_activate);

int namrbd_blk_resize(u64 volume_id, u64 generation, u64 size_bytes)
{
	struct namrbd_blk_dev *dev = namrbd_blk_lookup_default_device();

	if (!dev)
		return -ENODEV;
	return namrbd_blk_resize_device(dev->device_id, volume_id, generation, size_bytes);
}
EXPORT_SYMBOL(namrbd_blk_resize);

int namrbd_blk_configure_data_path(const char *address, u16 port,
				   u32 max_inflight_requests,
				   u64 max_inflight_bytes,
				   u32 max_io_size)
{
	struct namrbd_blk_dev *dev = namrbd_blk_lookup_default_device();

	if (!dev)
		return -ENODEV;
	return namrbd_blk_configure_data_path_device(dev->device_id, address, port,
						     1,
						     max_inflight_requests,
						     max_inflight_bytes,
						     max_io_size,
						     max_io_size,
						     NULL, NULL, NULL, NULL);
}
EXPORT_SYMBOL(namrbd_blk_configure_data_path);

void namrbd_blk_deactivate(u64 volume_id)
{
	struct namrbd_blk_dev *dev = namrbd_blk_lookup_default_device();

	if (!dev)
		return;
	namrbd_blk_deactivate_device(dev->device_id, volume_id);
}
EXPORT_SYMBOL(namrbd_blk_deactivate);

static int __init namrbd_blk_init(void)
{
	u64 default_size_bytes = size_mb * 1024ULL * 1024ULL;
	enum namrbd_no_path_retry_mode parsed_no_path_retry_mode;
	u32 parsed_no_path_retry_seconds;

	if (default_size_bytes < NAMRBD_BLOCK_SIZE)
		return -EINVAL;
	if (namrbd_parse_no_path_retry(no_path_retry, &parsed_no_path_retry_mode,
				       &parsed_no_path_retry_seconds)) {
		pr_err("namrbd_blk: invalid no_path_retry=%s expected fail|queue|<seconds>\n",
		       no_path_retry ? no_path_retry : "");
		return -EINVAL;
	}

	g_mgr.major = register_blkdev(0, NAMRBD_DISK_NAME_PREFIX);
	if (g_mgr.major < 0) {
		pr_err("namrbd_blk: register_blkdev failed err=%d\n", g_mgr.major);
		return g_mgr.major;
	}

	g_mgr.debugfs_root = debugfs_create_dir("namrbd", NULL);
	if (IS_ERR_OR_NULL(g_mgr.debugfs_root))
		g_mgr.debugfs_root = NULL;
	if (g_mgr.debugfs_root) {
		g_mgr.debugfs_devices_root = debugfs_create_dir("devices",
								 g_mgr.debugfs_root);
		if (IS_ERR_OR_NULL(g_mgr.debugfs_devices_root))
			g_mgr.debugfs_devices_root = NULL;
	}

	pr_info("namrbd_blk: initialized manager major=%d module_params size_mb=%llu nr_paths=%d default_active_lanes=%u max_gateway_connections=%u per_path_outstanding=%u data_max_io_size=%u sched_policy=%s down_mask=0x%lx degraded_mask=0x%lx draining_mask=0x%lx fail_path_id=%d no_path_retry=%s no_path_retry_mode=%s no_path_retry_seconds=%u no_path_requeue_delay_ms=%u no_path_max_queued_requests=%u trace_enabled=%d auto_devices=0\n",
		g_mgr.major, size_mb, nr_paths, default_active_lanes, max_gateway_connections,
		namrbd_per_path_outstanding_limit(), data_max_io_size,
		sched_policy ? sched_policy : "",
		down_mask, degraded_mask, draining_mask, fail_path_id,
		no_path_retry ? no_path_retry : "",
		namrbd_no_path_retry_mode_str(parsed_no_path_retry_mode),
		parsed_no_path_retry_seconds, no_path_requeue_delay_ms,
		no_path_max_queued_requests, trace_enabled);
	return 0;
}

static void __exit namrbd_blk_exit(void)
{
	struct namrbd_blk_dev *dev, *tmp;

	mutex_lock(&g_mgr.lock);
	list_for_each_entry_safe(dev, tmp, &g_mgr.device_list, list) {
		list_del(&dev->list);
		idr_remove(&g_mgr.devices, dev->device_id);
		ida_free(&g_mgr.disk_indexes, dev->disk_index);
		mutex_unlock(&g_mgr.lock);
		namrbd_free_device(dev);
		mutex_lock(&g_mgr.lock);
	}
	mutex_unlock(&g_mgr.lock);

	debugfs_remove_recursive(g_mgr.debugfs_root);
	g_mgr.debugfs_root = NULL;
	g_mgr.debugfs_devices_root = NULL;
	idr_destroy(&g_mgr.devices);
	ida_destroy(&g_mgr.disk_indexes);
	if (g_mgr.major > 0) {
		unregister_blkdev(g_mgr.major, NAMRBD_DISK_NAME_PREFIX);
		g_mgr.major = 0;
	}
	pr_info("namrbd_blk: unloaded\n");
}

module_init(namrbd_blk_init);
module_exit(namrbd_blk_exit);

MODULE_DESCRIPTION("NAMRBD blk-mq block device with multipath scheduler");
MODULE_AUTHOR("Taewoong Kim (taewoong.kim@gmail.com)");
MODULE_VERSION("1.0.0");
MODULE_LICENSE("GPL");
