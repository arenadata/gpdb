#
# Makefile for storage/ipc
#
# src/backend/storage/ipc/Makefile
#

subdir = src/backend/storage/ipc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = barrier.o dsm_impl.o dsm.o ipc.o ipci.o latch.o pmsignal.o procarray.o \
	procsignal.o shmem.o shmqueue.o shm_mq.o shm_toc.o signalfuncs.o \
	sinval.o sinvaladt.o standby.o lock_free_list.o lock_free_list_test.o

include $(top_srcdir)/src/backend/common.mk
