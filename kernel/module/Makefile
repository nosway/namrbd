obj-m += namrbd_ctrl.o
obj-m += namrbd_blk.o

KDIR ?= /lib/modules/$(shell uname -r)/build
# For kernel version 5.x (same source builds for 5.x/6.x via LINUX_VERSION_CODE in code)
KDIR_V5 = /lib/modules/5.15.0-33-generic/build

PWD := $(shell pwd)
KERNEL_CC ?= $(shell \
	if [ -r "$(KDIR)/include/generated/compile.h" ]; then \
		cc=$$(sed -n 's/.*LINUX_COMPILER "\(.*\)".*/\1/p' "$(KDIR)/include/generated/compile.h" | awk '{print $$1}' | head -1); \
		if [ -n "$$cc" ] && command -v "$$cc" >/dev/null 2>&1; then \
			echo "$$cc"; \
		fi; \
	fi)
KBUILD_CC := $(if $(KERNEL_CC),CC=$(KERNEL_CC),)
NAMRBD_HAVE_BLK_MQ_ALLOC_DISK_3_ARGS := $(shell \
	if grep -q "blk_mq_alloc_disk(set, lim, queuedata)" "$(KDIR)/include/linux/blk-mq.h" 2>/dev/null; then \
		echo 1; \
	else \
		echo 0; \
	fi)
NAMRBD_HAVE_BLK_MQ_UPDATE_NR_HW_QUEUES := $(shell \
	if grep -q "blk_mq_update_nr_hw_queues" "$(KDIR)/include/linux/blk-mq.h" 2>/dev/null; then \
		echo 1; \
	else \
		echo 0; \
	fi)
NAMRBD_HAVE_QUEUE_LIMITS_COMMIT_UPDATE := $(shell \
	if grep -q "queue_limits_commit_update" "$(KDIR)/include/linux/blkdev.h" 2>/dev/null; then \
		echo 1; \
	else \
		echo 0; \
	fi)

ccflags-y += -DNAMRBD_HAVE_BLK_MQ_ALLOC_DISK_3_ARGS=$(NAMRBD_HAVE_BLK_MQ_ALLOC_DISK_3_ARGS)
ccflags-y += -DNAMRBD_HAVE_BLK_MQ_UPDATE_NR_HW_QUEUES=$(NAMRBD_HAVE_BLK_MQ_UPDATE_NR_HW_QUEUES)
ccflags-y += -DNAMRBD_HAVE_QUEUE_LIMITS_COMMIT_UPDATE=$(NAMRBD_HAVE_QUEUE_LIMITS_COMMIT_UPDATE)

all:
	find $(PWD) -maxdepth 1 -type f -exec touch {} +
	$(MAKE) -C $(KDIR) M=$(PWD) $(KBUILD_CC) modules

# Build for 5.x: uses 5.x headers so LINUX_VERSION_CODE < 6.0.0 and 5.x blk-mq path is used
v5:
	find $(PWD) -maxdepth 1 -type f -exec touch {} +
	$(MAKE) -C $(KDIR_V5) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
