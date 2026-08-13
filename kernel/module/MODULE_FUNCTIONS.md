# NAMRBD 커널 모듈 함수별 동작 설명

이 문서는 `kernel/module` 하위에 구현된 두 커널 모듈(`namrbd_ctrl.c`, `namrbd_blk.c`)의 함수별 동작을 정리한다.

---

## 1. 모듈 구성 개요

| 파일 | 모듈명 | 역할 |
|------|--------|------|
| `namrbd_ctrl.c` | namrbd_ctrl.ko | Generic Netlink 제어 경로, REST 서버 설정, 볼륨 attach/detach (커널에서 REST 호출) |
| `namrbd_blk.c` | namrbd_blk.ko | blk-mq 기반 블록 디바이스, `/dev/namrbd0` 제공, 멀티패스 스케줄러, sysfs/debugfs |

**의존 관계**: `namrbd_ctrl`이 attach/detach 시 `namrbd_blk`의 `namrbd_blk_activate` / `namrbd_blk_deactivate`를 symbol로 호출한다. 로드 순서는 `namrbd_blk.ko` → `namrbd_ctrl.ko` 권장.

---

## 2. namrbd_ctrl.c (제어 경로 모듈)

### 2.1 서버 목록 관리

#### `namrbd_free_servers_locked(void)`
- **역할**: `namrbd_servers` 리스트에 등록된 모든 REST 서버 항목을 순회하며 `list_del` 후 `kfree`로 해제한다.
- **호출 조건**: 호출자 반드시 `namrbd_servers_lock` 뮤텍스를 잡은 상태에서만 호출해야 한다.
- **사용처**: `namrbd_cmd_config_rest_servers`(기존 목록 초기화 시), 모듈 종료 시.

#### `namrbd_first_server_locked(void)`
- **역할**: 서버 목록이 비어 있지 않으면 리스트의 첫 번째 `namrbd_rest_server` 포인터를 반환한다. 비어 있으면 `NULL` 반환.
- **호출 조건**: `namrbd_servers_lock`을 잡은 상태에서만 호출.

---

### 2.2 HTTP / JSON 처리

#### `namrbd_find_http_body(char *resp)`
- **역할**: HTTP 응답 문자열 `resp`에서 헤더와 본문 구분자인 `"\r\n\r\n"`를 찾고, 그 뒤의 본문 시작 주소(포인터)를 반환한다. 없으면 `NULL` 반환.
- **용도**: REST 응답에서 JSON 본문만 추출할 때 사용.

#### `namrbd_json_get_u64(const char *json, const char *key, u64 *out)`
- **역할**: 단순한 JSON 문자열 `json`에서 `"key"` 형태의 키를 찾고, 그 뒤의 콜론 다음에 나오는 10진수 숫자를 파싱해 `*out`에 저장한다. `simple_strtoull` 사용.
- **반환**: 성공 0, 키 없음 `-ENOENT`, 파싱 실패 `-EBADMSG`.
- **제한**: 중첩/이스케이프 등은 지원하지 않는 최소 구현이다.

#### `namrbd_json_get_string(const char *json, const char *key, char *out, size_t out_len)`
- **역할**: 단순 JSON 문자열에서 `"key":"value"` 형태의 문자열 값을 추출해 `out` 버퍼에 저장한다.
- **반환**: 성공 0, 키 없음 `-ENOENT`, 형식 오류 `-EBADMSG`, 버퍼 초과 `-E2BIG`.
- **제한**: 이스케이프된 따옴표나 중첩 구조는 지원하지 않는다.

#### `namrbd_parse_manifest_json(const char *json, u64 req_volume_id, struct namrbd_attach_manifest *m)`
- **역할**: attach manifest용 JSON 문자열을 파싱해 `namrbd_attach_manifest` 구조체를 채운다.
- **현재 반영 의미**:
  - `dataplane_endpoints` 배열의 object들을 `dataplane_paths[]` inventory로 파싱한다.
  - `path_id`, `gateway_id`, `address`, `port`, `use_tls`, `server_name`, `priority`를 path inventory에 직접 보존한다.
  - 즉 attach authority는 “단일 endpoint + count”가 아니라 “full endpoint inventory”다.
- **검증**:
  - `volume_id`는 JSON 문자열 형태의 **8자리 소문자 16진**(예: `00000065`)이며, 파싱된 값이 요청한 volume과 일치해야 한다.
  - `size_bytes`는 0이면 안 된다.
  - `block_size`는 0이면 안 되며 `u32` 범위여야 한다.
  - `dataplane_endpoints[].path_id`는 중복되면 안 된다.
- **현재 반영 필드**:
  - `volume_id`
  - `generation`
  - `size_bytes`
  - `block_size`
  - `attached_host_id`
  - `dataplane_paths[]`
  - `max_inflight_requests`
  - `max_inflight_bytes`
  - `max_io_size`
- **반환**: 성공 0, 실패 시 음수 에러 코드.

#### `namrbd_validate_manifest(const struct namrbd_attach_manifest *m, u64 req_volume_id, const char *host_id)`
- **역할**: 파싱이 끝난 manifest가 attach 요청 문맥과 일치하는지 최종 검증한다.
- **검증**:
  - manifest `volume_id`와 요청 `volume_id` 일치 여부
  - `size_bytes`, `block_size`의 유효성
  - `attached_host_id`가 attach 요청의 `host_id`와 일치하는지 여부
  - 첫 active `dataplane_path` endpoint와 `max_inflight_requests`, `max_inflight_bytes`, `max_io_size`의 유효성
- **반환**: 성공 0, 불일치 시 `-EBADMSG`, `-ERANGE`, `-EACCES` 등.

#### `namrbd_http_status_to_errno(const char *resp)`
- **역할**: HTTP 상태 라인을 검사해 커널 errno로 최소 매핑한다.
- **현재 매핑**:
  - `200/201/204` → 성공
  - `400` → `-EINVAL`
  - `401/403` → `-EACCES`
  - `404` → `-ENOENT`
  - `409` → `-EEXIST`
  - `5xx` → `-EREMOTEIO`
  - 그 외 → `-EPROTO`

#### `namrbd_http_simple_request(struct namrbd_rest_server *srv, const char *method, const char *path, const char *body, char *resp_out, size_t resp_out_len)`
- **역할**: 커널 소켓으로 해당 REST 서버에 HTTP 요청을 한 번 보내고 응답을 받는다.
  - TLS(`srv->use_tls`)가 true면 `-EOPNOTSUPP` 반환(미구현).
  - TCP 연결: `sock_create_kern` → `kernel_connect` (주소는 `in_aton(srv->address)`).
  - 요청: HTTP/1.1, Host, Connection: close, Content-Type: application/json, Content-Length, Bearer 토큰(있을 때), 본문(body)을 조합해 전송.
  - 응답 수신 후 상태 라인을 `namrbd_http_status_to_errno()`로 해석한다.
  - 성공 시 `resp_out`/`resp_out_len`이 유효하면 응답 전체를 복사한다.
- **메모리**: 요청/응답 버퍼는 내부에서 `kmalloc`/`kfree`로 처리한다.
- **반환**: 0 또는 음수 에러 코드.

---

### 2.3 REST attach/detach 및 블록 디바이스 연동

#### `namrbd_rest_fetch_manifest(struct namrbd_rest_server *srv, u64 volume_id, const char *host_id, struct namrbd_attach_manifest *m)`
- **역할**: `srv`의 API prefix와 `volume_id`로 `GET .../volumes/{id}/info` URL을 만들고, `namrbd_http_simple_request`로 호출한 뒤 응답 본문에서 manifest JSON을 파싱하고 `namrbd_validate_manifest()`까지 수행한다.
- **반환**: 성공 0, HTTP/파싱 실패 시 음수.

#### `namrbd_activate_block_device(struct namrbd_attach_manifest *m)`
- **역할**: `namrbd_blk_activate()`를 직접 호출해 manifest의 `volume_id`, `size_bytes`, `block_size`, `generation`으로 블록 디바이스를 활성화한다.
- **전제**: `namrbd_blk.ko`가 먼저 로드되어 있어야 한다.
- **반환**: `namrbd_blk_activate` 반환값.

#### `namrbd_deactivate_block_device(u64 volume_id)`
- **역할**: `namrbd_blk_deactivate()`를 직접 호출해 해당 `volume_id`로 블록 디바이스를 비활성화한다.

#### `namrbd_rest_attach(const char *host_id, u64 volume_id)`
- **역할**: attach 전체 흐름을 수행한다.
  1. 서버 락을 잡고 첫 번째 서버를 얻는다. 없으면 `-ENODEV`.
  2. `POST .../volumes/{id}/attach`에 `{"host_id":"..."}` 본문으로 HTTP 요청.
  3. 실패하면 여기서 반환.
  4. 성공 시 `namrbd_rest_fetch_manifest`로 info 응답에서 manifest 수집.
  5. `namrbd_activate_block_device`로 블록 디바이스 활성화.
  6. `namrbd_blk_configure_data_paths_device()`로 full path inventory와 credit 제한을 blk 모듈에 전달한다. 이 단계에서 runtime도 빈 endpoint나 중복 `path_id`를 다시 한 번 거절한다.

#### `namrbd_rest_detach(const char *host_id, u64 volume_id)`
- **역할**: detach 흐름을 수행한다.
  1. 첫 번째 서버에 `POST .../volumes/{id}/detach` + `{"host_id":"..."}` 호출.
  2. HTTP 성공이면 `namrbd_deactivate_block_device(volume_id)` 호출.
  3. HTTP 반환값(성공/실패)을 그대로 반환.

---

### 2.4 Netlink 명령 핸들러

#### `namrbd_cmd_config_rest_servers(struct sk_buff *skb, struct genl_info *info)`
- **역할**: `NAMRBD_CMD_CONFIG_REST_SERVERS` 명령을 처리한다. `NAMRBD_ATTR_SERVERS` 중첩 속성 안의 각 `NAMRBD_ATTR_SERVER_ENTRY`를 파싱해 `namrbd_rest_server`를 만들고 `namrbd_servers` 리스트에 추가한다. 기존 서버 목록은 `namrbd_free_servers_locked`로 먼저 비운다.
- **필수 속성**: 주소, 포트, API prefix. ID, TLS, Bearer 토큰은 선택.
- **반환**: 성공 0, 파싱/할당 실패 시 `-EINVAL` 등. 실패 시 이미 추가한 항목까지 모두 정리한다.

#### `namrbd_cmd_attach_volume(struct sk_buff *skb, struct genl_info *info)`
- **역할**: `NAMRBD_ATTR_ATTACH_REQ` 중첩 속성에서 `host_id`, `volume_id`를 꺼내 `namrbd_rest_attach(host_id, volume_id)`를 호출한 결과를 반환한다. 속성 없거나 파싱 실패 시 `-EINVAL` 반환.

#### `namrbd_cmd_detach_volume(struct sk_buff *skb, struct genl_info *info)`
- **역할**: `NAMRBD_ATTR_DETACH_REQ`에서 `host_id`, `volume_id`를 꺼내 `namrbd_rest_detach(host_id, volume_id)`를 호출한 결과를 반환한다.

#### `namrbd_cmd_update_path_plan(struct sk_buff *skb, struct genl_info *info)`
- **역할**: `NAMRBD_CMD_UPDATE_PATH_PLAN` 명령을 처리한다. userspace가 내려준 `device_id`와 `down/degraded/draining` 비트마스크를 읽어 특정 디바이스의 runtime path state를 갱신한다.
- **필수 속성**: `NAMRBD_ATTR_DEVICE_ID`
- **선택 속성**:
  - `NAMRBD_ATTR_DOWN_MASK`
  - `NAMRBD_ATTR_DEGRADED_MASK`
  - `NAMRBD_ATTR_DRAINING_MASK`
- **동작**:
  - 지정된 `device_id`에 대해 `namrbd_blk_update_path_masks_device()`를 호출한다.
  - 전달되지 않은 마스크는 `0`으로 간주한다.
- **용도**: `namrbdctl apply-volume-path-plan`이 gateway discovery 결과를 kernel dataplane runtime path state로 반영할 때 사용한다.

---

### 2.5 모듈 초기화/종료

#### `namrbd_ctrl_init(void)` (__init)
- **역할**: `namrbd_genl_family`를 등록해 Generic Netlink로 제어 명령을 받을 수 있게 한다.

#### `namrbd_ctrl_exit(void)` (__exit)
- **역할**: 서버 락을 잡고 `namrbd_free_servers_locked`로 서버 목록을 비운 뒤, Generic Netlink 패밀리 등록 해제.

---

## 3. namrbd_blk.c (블록 디바이스 모듈)

### 3.1 경로 상태 및 스케줄링

#### `namrbd_path_state_str(enum namrbd_path_state s)`
- **역할**: 경로 상태 열거형을 문자열 `"UP"`, `"DEGRADED"`, `"DOWN"`, `"DRAINING"`, `"UNKNOWN"`으로 변환해 반환한다.

#### `namrbd_path_eligible(enum req_op op, struct namrbd_path *p, bool prefer_up_only)`
- **역할**: 해당 경로 `p`가 I/O 디스패치 후보로 적합한지 판단한다. `DOWN`, `DRAINING`이면 false. `prefer_up_only`가 true이면 `UP`만 허용(쓰기는 가능하면 UP만 사용). `op`는 읽기/쓰기 구분용으로 사용된다.

#### `namrbd_has_up_path(struct namrbd_blk_dev *dev, ulong tried_mask)`
- **역할**: `tried_mask`로 아직 시도하지 않은 경로 중 `state == NAMRBD_PATH_UP`인 것이 하나라도 있으면 true를 반환한다. 스케줄러가 “UP만 선호”할지 결정할 때 사용한다.

#### `namrbd_pick_path(struct namrbd_blk_dev *dev, enum req_op op, ulong tried_mask)`
- **역할**: 정책(`dev->policy`)에 따라 다음에 사용할 경로를 하나 골라 반환한다.
  - **RR**: `dev->rr_cursor`부터 순환하며, eligible하고 아직 시도하지 않은 첫 경로를 선택하고 커서를 증가시킨다.
  - **EWMA**: eligible한 경로 중 `ewma_latency_ns`가 가장 작은 경로를 선택한다.
  - **least_inflight**(기본): eligible한 경로 중 `inflight` 카운트가 가장 작은 경로를 선택한다.
- **반환**: 선택된 `struct namrbd_path *` 또는 후보가 없으면 `NULL`.
- **주의**: `tried_mask`는 runtime array slot 기준이며, external status/manifest의 `path_id`와 직접 동일시하면 안 된다.

#### `namrbd_path_complete(struct namrbd_path *p, u64 latency_ns, bool retry)`
- **역할**: 해당 경로에서 I/O가 끝났을 때 호출한다. EWMA 지연(`ewma_latency_ns`)을 `latency_ns`로 업데이트(기존값이 0이면 그대로, 아니면 7:1 비율로 스무딩), `completed` 증가, `retry`가 true면 `retries`도 증가시킨다. `p->lock`으로 보호한다.

#### `namrbd_path_transition(struct namrbd_blk_dev *dev, struct namrbd_path *p, enum namrbd_path_state new_state, u32 err, u32 wire_status)`
- **역할**: 경로 상태 변화를 기록한다. 상태, 최근 errno/wire status, 상태 전이 횟수, 마지막 전이 시점을 갱신한다.

#### `namrbd_path_mark_failure(struct namrbd_blk_dev *dev, struct namrbd_path *p, u32 err, u32 wire_status)`
- **역할**: 경로 실패를 누적한다.
- **정책**:
  - 첫 실패 시 `DEGRADED`
  - 반복 실패 누적 시 `DOWN`
  - 최근 errno를 전역 통계에도 반영
  - lane map 재계산이 필요하면 remap reason을 `path_degraded` 또는 `path_down`으로 기록한다.

#### `namrbd_path_mark_success(struct namrbd_blk_dev *dev, struct namrbd_path *p)`
- **역할**: 경로 성공 시 연속 실패 카운트를 리셋하고, `DRAINING`이 아니면 `UP`으로 복구한다.
- **특징**: 상태가 실제로 복구되면 lane remap reason을 `path_recovered`로 기록한다.

---

### 3.2 I/O 처리 (blk-mq)

#### `namrbd_rw_request(struct namrbd_blk_dev *dev, struct request *rq, struct namrbd_path *path)`
- **역할**: 블록 요청 `rq`에 대해 실제 읽기/쓰기를 수행한다. 현재는 **RAM 백링 스토리지**만 사용한다.
  - `blk_rq_pos(rq) << 9`로 바이트 오프셋 계산.
  - `rq_for_each_segment`로 각 세그먼트를 돌며, `dev->data + pos`와 버퍼 간 `memcpy`로 읽기/쓰기. `dev->data_lock`으로 동기화.
  - 범위가 `dev->size_bytes`를 넘으면 `BLK_STS_IOERR`.
  - 모듈 파라미터 `fail_path_id`가 설정되어 있고 `path->path_id`가 그 값이면 의도적으로 `BLK_STS_IOERR` 반환(재시도/장애 시뮬레이션용).
- **반환**: `BLK_STS_OK` 또는 `BLK_STS_IOERR`.

#### `namrbd_data_path_request(struct namrbd_blk_dev *dev, struct request *rq, struct namrbd_path *path)`
- **역할**: blk request를 binary data-plane으로 전송한다. 현재 wire v1 경로는 선택된 path의 persistent TCP connection을 재사용한다.
- **동작**:
  - 선택된 `path`의 endpoint `address`, `port`와 volume-wide `max_io_size`, `max_inflight_requests`, `max_inflight_bytes`를 사용한다.
  - payload가 있는 READ/WRITE는 `max_data_io_size`로 제한하고, payload가 없는 DISCARD/WRITE_ZEROES는 gateway가 광고한 `max_io_size`까지 허용한다. 이 분리는 iSCSI adapter의 wire-safe zero-like 경로와 Ceph RBD의 별도 discard/write-zeroes queue limit 모델을 따른다.
  - path에 열린 socket이 없으면 connect하고, 성공하면 `path->sock`에 보관한다.
  - wire v1은 request를 `request_id` keyed pending table에 등록한 뒤 `path->io_lock`을 잡고 request frame/payload 송신만 직렬화한다.
  - path별 receive worker가 response frame/payload를 읽고 response opcode, `request_id`, `volume_id`, `generation`을 검증한 뒤 matching pending request를 completion 처리한다.
  - 모듈 파라미터 `per_path_outstanding`의 product/default 값은 `1`이므로 기본 동작은 path당 outstanding request 1개를 유지한다. 테스트에서는 이 값을 키워 single connection multiple outstanding 구조를 검증할 수 있다.
  - READ는 wire `READ` 요청을 보내고 응답 payload를 bio 세그먼트로 복사한다.
  - WRITE는 24-byte zeroed write tag + data payload를 wire `WRITE` 요청으로 보낸다.
  - credit 제한을 넘으면 즉시 실패한다.
  - data-plane 미설정 시 RAM backing 경로(`namrbd_rw_request`)로 fallback 한다.
- **반환**: 성공 시 `BLK_STS_OK`, 실패 시 `BLK_STS_IOERR`.
- **남은 제약**:
  - 같은 path connection 안에서는 FIFO 성격을 유지하지만, 다른 lane/gateway 사이의 ordering은 보장하지 않는다.
  - `per_path_outstanding > 1`은 gateway/SBS의 same-range write ordering, FLUSH/FUA, read-after-write visibility 규칙과 함께 검증해야 하는 실험 경로다.

#### `namrbd_data_path_request_errno(struct namrbd_blk_dev *dev, struct request *rq, struct namrbd_path *path, u32 *wire_status_out)`
- **역할**: `namrbd_data_path_request()`의 내부 구현으로, 단순 성공/실패 대신 errno와 wire status를 반환한다.
- **용도**: `queue_rq()`에서 retry/failover/statistics/path state 전이에 활용한다.

#### `namrbd_queue_rq(struct blk_mq_hw_ctx *hctx, const struct blk_mq_queue_data *bd)`
- **역할**: blk-mq의 `queue_rq` 콜백. 각 요청의 진입점이다.
  - `blk_mq_start_request` 후 통계(total/read/write) 증가, (선택) trace 출력.
  - **READ/WRITE**:
    - attach되지 않은 상태면 즉시 실패
    - `hctx->queue_num % active_lane_count` 기반으로 `lane_id`를 먼저 계산한다. `hctx`가 없으면 fallback RR cursor를 사용한다.
    - 해당 lane의 preferred/fallback path를 우선 사용하고, 필요하면 policy fallback으로 다른 eligible path를 선택한다.
    - 선택된 path에 대해 `namrbd_data_path_request_errno`를 실행한다.
    - 실패 시 `namrbd_path_mark_failure()`로 경로 상태를 갱신하고 다른 경로로 재시도(최대 `nr_paths`번)
    - 성공 시 `namrbd_path_mark_success()`로 경로를 `UP`로 복구
  - **FLUSH/DISCARD/WRITE_ZEROES**: attach 상태일 때만 성공 처리
  - 그 외 op는 `BLK_STS_NOTSUPP`.
  - 마지막에 성공/실패 통계 갱신, `blk_mq_end_request(rq, st)` 호출.
- **반환**: 항상 `BLK_STS_OK`(실제 완료/실패는 `blk_mq_end_request`에 전달).

### 3.2.1 Phase H no-path queueing

#### module parameter: `no_path_retry`
- **역할**: 모든 datapath가 unusable일 때 request를 즉시 실패시킬지, queue 후
  재시도할지 결정한다.
- **값**:
  - `fail`: no-path request를 `BLK_STS_IOERR`로 완료한다.
  - `queue`: no-path request를 실패시키지 않고 blk-mq에 지연 재시도 대상으로
    남긴다.
  - `<seconds>`: 지정한 초 동안 queue한 뒤 deadline을 넘으면 실패시킨다.
- **운영 의미**: `queue`는 availability-first 정책이다. gateway path가 복구되면
  queued I/O가 이어지지만, path가 복구되지 않으면 application I/O가 무기한
  block될 수 있다.

#### module parameter: `no_path_requeue_delay_ms`
- **역할**: no-path 상태에서 queued request를 다시 확인하는 최소 지연 시간을
  millisecond 단위로 제한한다.

#### module parameter: `no_path_max_queued_requests`
- **역할**: no-path queue에 둘 수 있는 request 수 상한이다. `0`은 무제한이다.

#### `namrbd_requeue_no_path(...)`
- **역할**: 현재 request가 no-path policy상 queue 가능한지 판단하고,
  가능하면 no-path 상태/counter를 갱신한 뒤 blk-mq 재시도 대상으로 돌려준다.
- **반영 상태**: `no_path_state=queueing`, `last_no_path_reason`,
  `last_no_path_op`, `last_no_path_eligible_paths`, `last_no_path_tried_mask`,
  `no_path_queued_reqs`, `no_path_requeued_reqs`.

#### `namrbd_fail_no_path(...)`
- **역할**: no-path policy가 `fail`이거나 timed retry deadline을 넘었거나,
  queue 상한을 넘은 request를 실패 완료한다.
- **반영 상태**: `no_path_state=failing`, `no_path_failed_reqs`,
  `last_no_path_reason`.

#### `namrbd_wake_no_path_queue(...)` / `namrbd_kick_no_path_queue(...)`
- **역할**: path recovery, datapath reconfigure, probe 결과 등으로 queued request를
  다시 dispatch할 수 있게 queue를 깨운다.
- **반영 상태**: queueing 중 wake되면 `no_path_state=recovering`과
  `last_no_path_wakeup_jiffies`를 갱신한다.

---

### 3.3 경로/큐/디스크 수명 주기

#### `namrbd_init_paths(struct namrbd_blk_dev *dev)`
- **역할**: `nr_paths`만큼 `namrbd_path` 배열을 할당하고, 각 경로의 `path_id`, `state`(모듈 파라미터 `down_mask`, `degraded_mask`, `draining_mask` 반영), `inflight`, `ewma_latency_ns`(초기 1ms), state lock, path I/O lock, pending request list, outstanding semaphore, persistent socket 상태를 초기화한다. `sched_policy` 문자열에 따라 `dev->policy`를 RR / EWMA / LEAST_INFLIGHT 중 하나로 설정한다. attach manifest가 가진 dataplane path count는 이후 data path configure 단계에서 실제 eligible path 수를 제한하는 데 사용된다.

#### `namrbd_blk_update_path_masks_device(u32 device_id, u64 path_plan_revision, u64 down_mask_bits, u64 degraded_mask_bits, u64 draining_mask_bits)`
- **역할**: 특정 `device_id`의 경로 집합에 runtime path-plan 비트마스크를 적용한다.
- **동작**:
  - `path_plan_revision`가 0이 아니면 revision-aware apply를 수행한다.
  - 이미 적용된 revision보다 작은 값이면 `-ESTALE`로 거부한다.
  - 같은 revision이 다시 오면 idempotent retry로 보고 no-op 처리한다.
  - revision 0은 legacy compatibility 용도로만 허용되며, 이미 versioned revision이 적용된 뒤에는 `-ESTALE`로 거부한다.
  - 디바이스를 찾은 뒤 각 `path_id`에 대해 비트마스크를 검사한다.
  - 우선순위는 `DOWN > DRAINING > DEGRADED > UP`이다.
  - 각 경로의 `consecutive_errors`, `last_errno`, `last_wire_status`를 정리하고 `namrbd_path_transition()`으로 상태를 반영한다.
- **용도**: generic netlink `update path plan` 명령이 attach 이후에도 active path set 축소/복구를 kernel dataplane에 반영할 수 있게 하는 runtime control 경계다.

#### `namrbd_cleanup_paths(struct namrbd_blk_dev *dev)`
- **역할**: 각 path의 persistent socket을 닫은 뒤 `dev->paths`를 `kfree`하고 `paths`/`nr_paths`를 초기화한다.

#### `namrbd_init_queue(struct namrbd_blk_dev *dev)`
- **역할**: blk-mq 태그셋을 설정(ops, `nr_hw_queues=1`, queue_depth, driver_data 등)하고 `blk_mq_alloc_tag_set` 호출. `queue_limits`에 logical/physical block size, io_min/io_opt, data I/O용 max_sectors, DISCARD/WRITE_ZEROES용 max sectors 등을 넣은 뒤 `blk_mq_alloc_disk`로 gendisk와 큐를 한 번에 할당하고, `dev->disk` / `dev->queue`에 저장한다.
- **중요 제약(현재 구현)**:
  - 초기 생성 시에는 안전한 기본값으로 시작하지만, attach/reconfigure control path가 active lane 수에 맞춰 `blk_mq_update_nr_hw_queues()`로 queue topology를 조정한다.
  - attach/probe 후에는 `queue_limits_start_update()` / `queue_limits_commit_update()`가 있는 커널에서 negotiated queue limit를 갱신한다. 일반 `max_hw_sectors`/`max_sectors`는 `data_max_io_size` cap을 유지하고, `discard_max_bytes`/`write_zeroes_max_bytes`는 gateway `max_io_size`를 따른다.
  - dispatch는 Linux `nbd.c`와 유사하게 `hctx`를 connection/lane affinity의 기준으로 사용한다.
  - wire v1은 async receive/completion worker를 갖췄지만, product/default `per_path_outstanding=1`로 동작한다. NBD처럼 connection당 multiple outstanding request를 허용하는 실험은 gateway/SBS의 same-range write ordering, FLUSH/FUA, read-after-write visibility 규칙을 설계한 뒤 작은 cap부터 검증하고 blk-mq `queue_depth`와 정렬한다.

#### `namrbd_cleanup_queue(struct namrbd_blk_dev *dev)`
- **역할**: `blk_mq_free_tag_set`만 수행한다. 디스크/큐 자체는 `namrbd_unregister_disk`에서 `put_disk`로 해제되므로, 여기서는 `dev->queue`/`dev->disk`를 NULL로만 둔다.

#### `namrbd_register_disk(struct namrbd_blk_dev *dev)`
- **역할**: `register_blkdev(0, "namrbd")`로 major 번호를 받고, 이미 `namrbd_init_queue`에서 만든 `dev->disk`에 major, first_minor, minors, fops, private_data, disk_name을 설정한 뒤 `set_capacity`로 용량을 넣고 `add_disk`로 블록 레이어에 등록한다. `add_disk` 실패 시 `unregister_blkdev` 후 에러 반환.

#### `namrbd_unregister_disk(struct namrbd_blk_dev *dev)`
- **역할**: `del_gendisk` → `put_disk`로 디스크와 큐를 해제하고, `dev->disk`/`dev->queue`를 NULL로 둔 뒤, `dev->major`가 유효하면 `unregister_blkdev`로 major를 해제한다.

---

### 3.4 sysfs 속성

다음 함수들은 디스크 디바이스에 붙는 읽기 전용 sysfs 속성의 show 콜백이다. `disk_to_dev(dev->disk)`에 대해 `device_create_file`로 등록된다.

- **`volume_state_show`**: attach 여부에 따라 `"ATTACHED"` 또는 `"DETACHED"` 문자열을 반환.
- **`size_bytes_show`**: `g_dev->size_bytes`를 10진수 문자열로 반환.
- **`block_size_show`**: 논리 블록 크기 `NAMRBD_BLOCK_SIZE`(4096)를 반환.
- **`generation_show`**: 현재 attach된 volume generation을 반환.
- **`dataplane_show`**: 현재 data-plane 주소/포트와 negotiated limit를 반환.
- **`inflight_show`**: 현재 data-plane inflight request/byte 수를 반환.
- **`active_policy_show`**: 현재 스케줄 정책 `rr` / `ewma` / `least_inflight` 문자열 반환.
- **`path_states_show`**: 각 경로의 `path_id`와 상태 문자열을 나열한 한 줄을 반환(예: `0:UP 1:DEGRADED `).

#### `namrbd_sysfs_create(struct namrbd_blk_dev *dev)` / `namrbd_sysfs_remove(struct namrbd_blk_dev *dev)`
- **역할**: 위 속성들을 `disk_to_dev(dev->disk)`에 생성/제거한다. create 실패 시 이전에 생성한 속성부터 역순으로 제거한다.

---

### 3.5 debugfs

#### `namrbd_debugfs_stats_show(struct seq_file *m, void *v)`
- **역할**: `attached`, `volume_id`, `generation`, `chunk_size_bytes`, `max_io_size`, `max_data_io_size`, `max_data_io_bytes`, `max_discard_bytes`, `max_write_zeroes_bytes`, `active_path_count`, `active_lane_count`, `per_path_outstanding`, `nr_hw_queues`, `sched_policy`, `rr_cursor`, `applied_path_plan_revision`, `lane_remap_count`, `last_lane_remapped_lanes`, `last_lane_remap_reason`, `last_lane_remap_jiffies`, `last_selected_lane_id`, `last_selected/completed/failed/failover_from/failover_to path id`, `down/degraded/draining mask`, `up/degraded/down/draining path count`와 함께 `total_reqs`, `read_reqs`, `write_reqs`, `retry_reqs`, `timeout_reqs`, `failed_reqs`, `completed_reqs`, `path_failover_reqs`, `probe_failures`, `path_state_changes`, `data_inflight_*`, `last_errno`를 출력한다. `/sys/kernel/debug/namrbd/stats`에 해당한다.

#### `namrbd_debugfs_paths_show(struct seq_file *m, void *v)`
- **역할**: 각 경로별로 `id`, `state`, `connected`, `inflight`, `pending`, `outstanding_limit`, `ewma_ns`, `completed`, `retries`에 더해 `conn_opens`, `conn_resets`, `consecutive_errors`, `last_errno`, `last_wire_status`, `state_changes`, `last_transition_jiffies`를 출력한다. `/sys/kernel/debug/namrbd/paths`에 해당한다.

#### `namrbd_blk_get_status(u32 device_id, struct namrbd_blk_status *out)`
- **역할**: netlink status/path-status 응답에 active path와 lane 상태를 복사한다. path 항목에는 gateway endpoint 정보와 함께 `connected`, `inflight`, `pending`, `outstanding_limit`, `completed`, `retries`, `conn_opens`, `conn_resets`를 포함해 fio 비교 스크립트가 debugfs와 같은 기준으로 path별 분산/연결 재사용 상태를 저장할 수 있게 한다.

#### `namrbd_debugfs_lanes_show(struct seq_file *m, void *v)`
- **역할**: 각 active lane의 `lane=<id> preferred_path_id=<path> fallback_path_id=<path|none> readiness=<stable|degraded_with_up_fallback|degraded_without_up_fallback|unavailable>`를 출력한다. baseline 정책은 `DEGRADED` preferred lane을 유지하되, fallback은 가능하면 `UP` path를 먼저 가리키고, `DOWN` preferred는 lane remap으로 제거한다. lane map이 바뀌면 `lane_remap_count`와 `last_lane_remap_*`가 함께 갱신된다. `/sys/kernel/debug/namrbd/lanes`에 해당한다.

#### `namrbd_debugfs_init(struct namrbd_blk_dev *dev)` / `namrbd_debugfs_cleanup(struct namrbd_blk_dev *dev)`
- **역할**: `debugfs_create_dir("namrbd", NULL)` 아래에 `stats`, `paths` 파일을 만들고, 해제 시 디렉터리 전체를 `debugfs_remove_recursive`로 제거한다.

---

### 3.6 외부 공개 API (namrbd_ctrl에서 사용)

#### `namrbd_blk_activate(u64 volume_id, u64 size_bytes, u32 block_size, u32 chunk_size_bytes, u64 generation)`
- **역할**: 컨트롤 모듈이 attach 시 호출한다. 새 크기의 RAM 백 버퍼를 `vmalloc`하고, 큐를 quiesce한 뒤 `g_dev->data`, `size_bytes`, `volume_id`, `generation`, `chunk_size_bytes`, `attached`를 갱신하고 이전 버퍼를 `vfree`한다. 그 다음 `set_capacity`로 디스크 용량을 갱신하고 큐를 unquiesce한다. `block_size`는 반드시 `NAMRBD_BLOCK_SIZE`(4096)와 같아야 하며, `chunk_size_bytes`는 `block_size` 이상이고 그 배수여야 한다.
- **반환**: 0 또는 `-ENODEV` / `-EINVAL` / `-ENOMEM`. `EXPORT_SYMBOL`로 내보낸다.

#### `namrbd_blk_configure_data_path(const char *address, u16 port, u32 max_inflight_requests, u64 max_inflight_bytes, u32 max_io_size)`
- **역할**: 컨트롤 모듈이 attach 직후 호출한다. gateway binary data-plane 주소/포트와 credit 제한을 blk 모듈에 반영한다.
- **동작**:
  - `data_address`, `data_port`, `max_inflight_requests`, `max_inflight_bytes`, `max_io_size`를 저장한다.
  - probe work를 즉시 스케줄해 `PATH_PROBE`로 현재 경로 상태와 negotiated limit를 갱신한다.
- **반환**: 0 또는 `-EINVAL`. `EXPORT_SYMBOL`로 내보낸다.

#### `namrbd_blk_get_status(u32 device_id, struct namrbd_blk_status *out)`
- **역할**: 특정 디바이스의 attach 상태와 함께 runtime multipath 요약 상태를 채운다.
- **반영 필드**:
  - `device_id`, `disk_name`, `attached`, `volume_id`, `generation`
  - `path_count`
  - `down_mask`
  - `degraded_mask`
  - `draining_mask`
  - path별 `path_id`, `state`, `consecutive_errors`, `last_errno`, `last_wire_status`
- **용도**: generic netlink `get status` / `list devices` 응답이 userspace에 현재 kernel path-plan 상태를 노출할 수 있게 한다. `namrbdctl status`, `namrbdctl list-devices`가 이 값을 사용한다.

#### `namrbd_blk_list_devices(struct namrbd_blk_status *out, u32 max_entries, u32 *count_out)`
- **역할**: 등록된 디바이스 전체의 요약 상태를 배열 형태로 채운다.
- **특징**: 각 엔트리에 `namrbd_blk_get_status()`와 같은 runtime path-plan 필드(`path_count`, `down_mask`, `degraded_mask`, `draining_mask`)와 path별 상세 상태 배열이 포함된다.

#### `namrbd_probe_data_path(struct namrbd_blk_dev *dev)` / `namrbd_probe_workfn(struct work_struct *work)`
- **역할**: binary data-plane에 `PATH_PROBE`를 보내 현재 path health와 negotiated limit를 갱신한다.
- **현재 정책**:
  - probe 실패 시 path를 `DOWN`으로 전이
  - probe 성공 시 path를 `UP`으로 복구
  - workqueue 기반으로 주기적으로 다시 probe 한다

#### `namrbd_blk_deactivate(u64 volume_id)`
- **역할**: detach 시 호출한다. 큐를 quiesce한 뒤, `volume_id`가 0이거나 현재 디바이스의 volume_id와 같으면 `attached`를 false로 두고 `volume_id`/`generation`을 0으로 만든다. data-plane 주소/포트도 비우고 probe work를 취소한다. 데이터 버퍼는 해제하지 않고, 이후 attach 시 다시 활성화될 수 있도록 둔다. `EXPORT_SYMBOL`로 내보낸다.

---

### 3.7 모듈 초기화/종료

#### `namrbd_blk_init(void)` (__init)
- **역할**: 전역 디바이스 `g_dev` 할당, `size_bytes`(size_mb 기반), `data`(vmalloc), 경로 초기화, 큐/디스크 초기화, 디스크 등록, sysfs/debugfs 생성. 한 단계라도 실패하면 이전 단계까지 롤백하고 에러 반환.

#### `namrbd_blk_exit(void)` (__exit)
- **역할**: debugfs 제거 → sysfs 제거 → 디스크 해제 → 큐/태그셋 정리 → 경로 정리 → `data` vfree → `g_dev` 해제. 순서는 init의 역순이다.

---

## 4. 모듈 파라미터 (namrbd_blk.c)

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `size_mb` | ullong | 64 | RAM 백링 스토리지 크기(MiB). |
| `nr_paths` | int | 2 | 시뮬레이션 경로 개수(1~NAMRBD_MAX_PATHS). |
| `default_active_lanes` | uint | 2 | 기본 active dispatch lane 수. 0이면 기본 제한 없음. |
| `max_gateway_connections` | uint | NAMRBD_MAX_PATHS | 최대 active dispatch lane/gateway connection 수. |
| `per_path_outstanding` | uint | 1 | path당 persistent TCP connection 위에서 허용할 outstanding request 수. 기본값 1은 product/default 안전 경로이고, 16 등은 성능 실험용. |
| `sched_policy` | charp | "least_inflight" | 스케줄 정책: `rr`, `ewma`, `least_inflight`. |
| `down_mask` | ulong | 0 | 비트마스크; 해당 비트의 경로를 DOWN으로 둠. |
| `degraded_mask` | ulong | 0 | 해당 비트의 경로를 DEGRADED로 둠. |
| `draining_mask` | ulong | 0 | 해당 비트의 경로를 DRAINING으로 둠. |
| `fail_path_id` | int | -1 | 0 이상이면 해당 path_id로 처리한 요청을 강제 실패(재시도 테스트용). |
| `no_path_retry` | charp | "fail" | no-path retry 정책: `fail`, `queue`, 또는 초 단위 숫자. |
| `no_path_requeue_delay_ms` | uint | 1000 | no-path queued request 재확인 지연(ms). |
| `no_path_max_queued_requests` | uint | 0 | no-path queued request 최대 개수. 0이면 무제한. |
| `trace_enabled` | bool | false | true면 `namrbd_queue_rq` 등에서 `pr_debug` 기반 상세 디버그 로그를 출력. |
| `data_max_io_size` | uint | 131072 | payload가 있는 READ/WRITE 요청의 최대 크기. DISCARD/WRITE_ZEROES limit와 분리되어 커널 payload buffer 부담을 기존 수준으로 유지한다. |

로드가 성공하면 `namrbd_blk_init()`은 위 module parameter의 실제 값을
`namrbd_blk: initialized manager module_params ...` 커널 로그로 남긴다.

---

## 5. 참고

- UAPI 및 Netlink 속성 정의: `kernel/uapi/namrbd_netlink.h`
- 상세 설계: 프로젝트 루트의 `03_kernel_driver_design.md`, `10_protocol_and_kernel_control_implementation_status.md`
