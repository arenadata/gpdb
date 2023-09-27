top_builddir = ../../../..
subdir = src/backend/catalog


include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/Makefile.mock
include $(top_srcdir)/src/backend/mock.mk

TARGETS += storage_tablespace storage_pending


storage_tablespace.t: $(top_srcdir)/src/backend/catalog/storage_tablespace.o
	make -C $(top_srcdir)/src/backend/catalog/ && \
	$(CC) $(CFLAGS) $(LDFLAGS) $(CMOCKERY_OBJS) $(CPPFLAGS) \
		$(top_srcdir)/src/backend/catalog/storage_tablespace.o \
		storage_tablespace_test.c \
		-o storage_tablespace_test.o && ./storage_tablespace_test.o

storage_pending.t: \
	$(MOCK_DIR)/backend/utils/mmgr/dsa_mock.o \
	$(MOCK_DIR)/backend/storage/lmgr/lwlock_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o

check: storage_tablespace.t storage_pending.t