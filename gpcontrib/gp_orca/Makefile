MODULE_big = gp_orca

OBJS = gporca.o \
	funcs.o \

PG_CPPFLAGS = \
	-I$(top_srcdir)/src/backend/gporca/libgpos/include $(CPPFLAGS) \
	-I$(top_srcdir)/src/backend/gporca/libgpopt/include $(CPPFLAGS) \
	-I$(top_srcdir)/src/backend/gporca/libnaucrates/include $(CPPFLAGS) \
	-I$(top_srcdir)/src/backend/gporca/libgpdbcost/include $(CPPFLAGS) \

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp_orca
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

install: install-gp-orca

install-gp-orca: $(DESTDIR)$(datadir)/postgresql.conf.sample
	sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'gp_orca'/g" $(DESTDIR)$(datadir)/postgresql.conf.sample
