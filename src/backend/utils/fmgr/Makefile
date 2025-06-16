#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/fmgr
#
# IDENTIFICATION
#    src/backend/utils/fmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/fmgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

<<<<<<< HEAD
OBJS = dfmgr.o fmgr.o funcapi.o deprecated.o
=======
OBJS = \
	dfmgr.o \
	fmgr.o \
	funcapi.o
>>>>>>> BISECT_HEAD

override CPPFLAGS += -DDLSUFFIX=\"$(DLSUFFIX)\"

include $(top_srcdir)/src/backend/common.mk
