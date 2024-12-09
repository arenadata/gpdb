MODULE_big = orca

GPOPT_OBJS = $(shell cat gpopt/objfiles.list)

GPORCA_OBJS = $(shell cat gporca/objfiles.list)

OBJS = $(GPOPT_OBJS) $(GPORCA_OBJS) \
	orca.o \
	funcs.o \
	orca_entry.o \

PG_CPPFLAGS = \
	-I$(top_srcdir)/gpcontrib/orca/gporca/libgpos/include \
	-I$(top_srcdir)/gpcontrib/orca/gporca/libgpopt/include \
	-I$(top_srcdir)/gpcontrib/orca/gporca/libnaucrates/include $(CPPFLAGS) \
	-I$(top_srcdir)/gpcontrib/orca/gporca/libgpdbcost/include $(CPPFLAGS) \

PG_CXXFLAGS = -Werror -Wextra -Wpedantic -fno-omit-frame-pointer $(CXXFLAGS)

SHLIB_LINK += -lxerces-c

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/orca
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

install: install-orca

install-orca: $(DESTDIR)$(datadir)/postgresql.conf.sample
	sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'orca'/g" $(DESTDIR)$(datadir)/postgresql.conf.sample
