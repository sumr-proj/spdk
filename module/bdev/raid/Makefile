#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2015 Intel Corporation.
#  All rights reserved.
#

SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 6
SO_MINOR := 0
# ->
# CFLAGS += $(ENV_CFLAGS)
# <-
CFLAGS += -I$(SPDK_ROOT_DIR)/lib/bdev/ 
# -I$(SPDK_ROOT_DIR)/lib/env_dpdk/
C_SRCS = service.c bdev_raid.c bdev_raid_rpc.c raid0.c raid1.c concat.c

ifeq ($(CONFIG_RAID5F),y)
C_SRCS += raid5f.c
endif

LIBNAME = bdev_raid

SPDK_MAP_FILE = $(SPDK_ROOT_DIR)/mk/spdk_blank.map

include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk

# ->
# LIBDPDK_PKGCONFIG = $(call pkgconfig_filename,spdk_dpdklibs)

# $(LIBDPDK_PKGCONFIG): $(PKGCONFIG) $(PKGCONFIG_INST)
# 	$(Q)$(SPDK_ROOT_DIR)/scripts/pc_libs.sh \
# 		"-L$(DPDK_LIB_DIR) $(DPDK_LIB_LIST:%=-l%)" "" DPDK spdk_dpdklibs > $@
# 	$(Q)sed -i.bak '5s,.*,Requires: $(DEPDIRS-$(LIBNAME):%=spdk_%) spdk_dpdklibs,' $(PKGCONFIG) ; rm $(PKGCONFIG).bak
# 	$(Q)sed -i.bak '5s,.*,Requires: $(DEPDIRS-$(LIBNAME):%=spdk_%) spdk_dpdklibs,' $(PKGCONFIG_INST) ; rm $(PKGCONFIG_INST).bak

# _install_dpdklibs: $(LIBDPDK_PKGCONFIG)
# 	@$(call pkgconfig_install,$(LIBDPDK_PKGCONFIG))

# _uninstall_dpdklibs: $(LIBDPDK_PKGCONFIG)
# 	@$(call pkgconfig_uninstall,$(LIBDPDK_PKGCONFIG))

# all: $(LIBDPDK_PKGCONFIG)
# install: _install_dpdklibs
# uninstall: _uninstall_dpdklibs
# <-

