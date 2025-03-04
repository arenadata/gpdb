subdir=src/backend/nodes
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=lfl

include $(top_srcdir)/src/backend/mock.mk

lfl.t: \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \

