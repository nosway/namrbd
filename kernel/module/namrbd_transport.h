/* SPDX-License-Identifier: GPL-2.0-only */
#ifndef _NAMRBD_TRANSPORT_H_
#define _NAMRBD_TRANSPORT_H_

#include <linux/in.h>
#include <linux/inet.h>
#include <linux/net.h>
#include <linux/string.h>
#include <linux/types.h>

#include <net/sock.h>

#define NAMRBD_TRANSPORT_ADDR_LEN 64
#define NAMRBD_TRANSPORT_SERVER_NAME_LEN 128
#define NAMRBD_TRANSPORT_GATEWAY_ID_LEN 64
#define NAMRBD_TRANSPORT_MAX_PATHS 16

struct namrbd_transport_endpoint {
	char address[NAMRBD_TRANSPORT_ADDR_LEN];
	u16 port;
	bool use_tls;
	char server_name[NAMRBD_TRANSPORT_SERVER_NAME_LEN];
};

struct namrbd_transport_path {
	u32 path_id;
	u32 priority;
	char gateway_id[NAMRBD_TRANSPORT_GATEWAY_ID_LEN];
	struct namrbd_transport_endpoint endpoint;
};

static inline int namrbd_transport_connect(const struct namrbd_transport_endpoint *ep,
					   struct socket **sock_out)
{
	struct socket *sock;
	struct sockaddr_in sin = { 0 };
	int ret;

	if (!ep || !sock_out || !ep->address[0] || !ep->port)
		return -EINVAL;
	if (ep->use_tls)
		return -EOPNOTSUPP;

	ret = sock_create_kern(&init_net, AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret < 0)
		return ret;

	sin.sin_family = AF_INET;
	sin.sin_port = htons(ep->port);
	sin.sin_addr.s_addr = in_aton(ep->address);
	ret = kernel_connect(sock, (struct sockaddr *)&sin, sizeof(sin), 0);
	if (ret < 0) {
		sock_release(sock);
		return ret;
	}

	*sock_out = sock;
	return 0;
}

static inline int namrbd_transport_send_all(struct socket *sock, const u8 *buf, size_t len)
{
	struct msghdr msg = { 0 };
	struct kvec iov;
	size_t sent = 0;
	int ret;

	while (sent < len) {
		iov.iov_base = (void *)(buf + sent);
		iov.iov_len = len - sent;
		ret = kernel_sendmsg(sock, &msg, &iov, 1, iov.iov_len);
		if (ret <= 0)
			return ret ? ret : -EIO;
		sent += ret;
	}
	return 0;
}

static inline int namrbd_transport_recv_all(struct socket *sock, u8 *buf, size_t len)
{
	struct msghdr msg = { 0 };
	struct kvec iov;
	size_t recvd = 0;
	int ret;

	while (recvd < len) {
		iov.iov_base = buf + recvd;
		iov.iov_len = len - recvd;
		ret = kernel_recvmsg(sock, &msg, &iov, 1, iov.iov_len, 0);
		if (ret <= 0)
			return ret ? ret : -EIO;
		recvd += ret;
	}
	return 0;
}

#endif /* _NAMRBD_TRANSPORT_H_ */
