# SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)
# mirror eBPF 项目 - 顶层 Makefile

CLANG      ?= clang
LLVM_STRIP ?= llvm-strip
CC         ?= cc
V          ?= 0

# 目录定义
ROOT_DIR    := $(abspath .)
OUTPUT      := $(ROOT_DIR)/.output
LIBBPF_SRC  := $(ROOT_DIR)/libbpf/src
BPFTOOL_SRC := $(ROOT_DIR)/bpftool/src

# 架构检测
ARCH := $(shell uname -m | sed 's/x86_64/x86/' \
                         | sed 's/aarch64/arm64/' \
                         | sed 's/ppc64le/powerpc/' \
                         | sed 's/mips.*/mips/' \
                         | sed 's/arm.*/arm/' \
                         | sed 's/riscv64/riscv/')

# 预构建产物路径
LIBBPF_OBJ  := $(OUTPUT)/libbpf.a
BPFTOOL     := $(OUTPUT)/bpftool/bootstrap/bpftool
VMLINUX_BTF := /sys/kernel/btf/vmlinux
VMLINUX_H   := $(ROOT_DIR)/vmlinux/$(ARCH)/vmlinux.h

# 输出控制
ifeq ($(V),1)
    Q =
    msg = @true
else
    Q = @
    msg = @printf '  %-8s %s\n' "$(1)" "$(2)";
endif

.PHONY: all prebuild vmlinux clean submodules help

.DEFAULT_GOAL := help

help:
	@echo "可用命令:"
	@echo "  make prebuild  - 构建 libbpf 和 bpftool (首次运行必做)"
	@echo "  make all       - 编译 mirror 程序"
	@echo "  make vmlinux   - 从当前系统内核提取 vmlinux.h"
	@echo "  make clean     - 清理所有构建产物"

all: prebuild
	$(Q)$(MAKE) -C src all

prebuild: libbpf bpftool
	@echo "  OK       libbpf.a 和 bpftool 已就绪"

vmlinux: bpftool
	$(call msg,VMLINUX,$(VMLINUX_H))
	@if [ ! -f $(VMLINUX_BTF) ]; then \
		echo "错误: $(VMLINUX_BTF) 不存在，内核可能不支持 BTF"; \
		exit 1; \
	fi
	$(Q)mkdir -p $(dir $(VMLINUX_H))
	$(Q)$(BPFTOOL) btf dump file $(VMLINUX_BTF) format c > $(VMLINUX_H)

submodules:
	@if [ ! -f "$(LIBBPF_SRC)/Makefile" ]; then \
		echo "  INIT     正在初始化 Git 子模块..."; \
		git submodule update --init --recursive; \
	fi

libbpf: submodules $(LIBBPF_OBJ)

bpftool: submodules $(BPFTOOL)

$(OUTPUT)/libbpf $(OUTPUT)/bpftool:
	$(Q)mkdir -p $@

$(LIBBPF_OBJ): $(wildcard $(LIBBPF_SRC)/*.[ch] $(LIBBPF_SRC)/Makefile) | $(OUTPUT)/libbpf
	$(call msg,LIB,libbpf.a)
	$(Q)$(MAKE) -C $(LIBBPF_SRC) BUILD_STATIC_ONLY=1 \
		OBJDIR=$(OUTPUT)/libbpf DESTDIR=$(OUTPUT) \
		INCLUDEDIR= LIBDIR= UAPIDIR= install

$(BPFTOOL): | $(OUTPUT)/bpftool
	$(call msg,BPFTOOL,bpftool)
	$(Q)$(MAKE) ARCH= -C $(BPFTOOL_SRC) OUTPUT=$(OUTPUT)/bpftool/ bootstrap

clean:
	$(call msg,CLEAN,mirror)
	$(Q)$(MAKE) -C src clean
	$(Q)rm -rf $(OUTPUT)
