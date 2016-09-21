# *-------------------------------------------------------------------------
# *
# * Makefile
# *
# *
# *
# * Copyright (c) 2013, Konstantin Knizhnik, ww.garret.ru
# * Author: Konstantin Knizhnik <knizhnik@garret.ru>
# *
# *	  $Id: Makefile 28 2013-10-10 17:18:31Z lptolik $
# *
# *-------------------------------------------------------------------------

MODULE_big = hash_fdw

HASH_FDW_VERSION=1.0

OBJS =  hash_if.o hash_fdw.o shm_alloc.o

EXTENSION = hash_fdw
DATA = hash_fdw--1.0.sql
REGRESS = create span operators math datetime transform scalarop grandagg groupbyagg gridagg windowagg hashagg cumagg sort spec drop

SHLIB_LINK += $(filter -lm, $(LIBS))
USE_PGXS=1
ifdef USE_PGXS
PG_CONFIG = /usr/local/psql-9.5.3/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hash_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

distrib:
	rm -f *.o
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
	cd .. ; tar --exclude=.svn -chvzf hash_fdw-$(HASH_FDW_VERSION).tar.gz hash_fdw
