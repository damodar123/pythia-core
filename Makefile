# For the non-MPI version, call as `make`.
#
# For the MPI version, call as `make CPPFLAGS=-DENABLE_MPI`.
# Do `export MV2_ENABLE_AFFINITY=0` before starting for the MPI operator.
# 
.PHONY: all clean doc distclean unit_tests drivers tags util/timestamp.o

include system.inc

override CPPFLAGS+=-Idist/include/

CXXFLAGS+=-g 
CXXFLAGS+=-O0 -Wall
#CXXFLAGS+=-O3 -Wall

LDFLAGS+=-Ldist/lib/
LDLIBS+=-lconfig++ -lpthread -lrt -lbz2

ifneq ($(findstring ENABLE_RDMA,$(CPPFLAGS)),)
LDLIBS+=-libverbs
endif

# MPI needs different CXX
#
ifneq ($(findstring ENABLE_MPI,$(CPPFLAGS)),)
CXX=mpic++
EXECSUFFIX=.with-mpi
else
EXECSUFFIX=.no-mpi
endif

# Bitonic sort needs SSE4.2 support.
# 
ifneq ($(findstring BITONIC_SORT,$(CPPFLAGS)),)
CXXFLAGS+=-msse4.2
endif

SHELL=/bin/bash		# for HOSTTYPE variable, below
ifeq ($(shell echo $$HOSTTYPE),sparc)
LDLIBS+=-lcpc -lsocket -lnsl
CXXFLAGS+=-mcpu=ultrasparc
endif

# Add libraries
#
ifneq ($(findstring ENABLE_NUMA,$(CPPFLAGS)),)
LDLIBS+=-lnuma
endif
ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
LDLIBS+=-lhdf5
endif
ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
LDLIBS+=-lfastbit
endif

CC=$(CXX)
CFLAGS=$(CXXFLAGS)

FILES = schema.o hash.o \
	ProcessorMap.o Barrier.o \
	perfcounters.o \
	query.o \
	util/hashtable.o \
	util/buffer.o \
	visitors/recursivedestroy.o \
	visitors/recursivefree.o \
	visitors/prettyprint.o \
	visitors/threadinit.o \
	visitors/threadclose.o \
	operators/base.o \
	operators/mapwrapper.o \
	operators/filter.o \
	operators/sortlimit.o \
	operators/genericaggregate.o \
	operators/aggregatecount.o \
	operators/aggregatesum.o \
	operators/scan.o \
	operators/partitionedscan.o \
	operators/repeatpartitionedscan.o \
	operators/parallelscan.o \
	operators/repeatparallelscan.o \
	operators/merge.o \
	operators/join.o \
	operators/shuffle.o \
	operators/data_sender_shuffle_tcp.o \
	operators/data_sender_bcast_tcp.o \
	operators/data_recver_tcp.o \
	operators/cycleaccountant.o \
	operators/timeaccountant.o \
	util/affinitizer.o \
	operators/project.o \
	comparator.o \
	conjunctionevaluator.o \
	rawcompfns.o \
	operators/memsegmentwriter.o \
	operators/loaders/table.o \
	operators/loaders/sfmt/SFMT.o \
	operators/loaders/parser.o \
	operators/loaders/loader.o \
	operators/checker_callstate.o \
	operators/printer_tuplecount.o \
	operators/printer_getnextcallcount.o \
	operators/generator_int.o \
	operators/generator_zipf.o \
	operators/printer_perfcnt.o \
	operators/bitentropy.o \
	operators/threadidprepend.o \
	util/numaasserts.o \
	util/numaallocate.o \
	operators/consume.o \
	operators/partition.o \
	operators/indexjoin.o \
	util/cpufreq.o \
	operators/perfcount_scanstart.o \
	operators/perfcount_getnext.o \
	operators/perfcount_all.o \
	operators/sort.o \
	util/topology.o \
	util/tcpsocket.o \
	util/timestamp.o \

ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
FILES += \
	operators/hdf5scan.o \

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
FILES += \
	operators/hdf5index.o \
	operators/hdf5random.o \

endif
endif

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
FILES += \
	operators/fastbitscan.o \
	operators/hdf5random.o \

endif

ifneq ($(findstring ENABLE_MPI,$(CPPFLAGS)),)
FILES += \
	operators/data_sender_shuffle_mpi.o \
	operators/data_recver_mpi.o \
	operators/data_sender_bcast_mpi.o \
	operators/data_recver_bcast_mpi.o \

endif

ifneq ($(findstring ENABLE_RDMA,$(CPPFLAGS)),)
FILES += \
	operators/data_sender_bcast_ud_sr.o \
	operators/data_sender_shuffle_ud_sr.o \
	operators/data_recver_ud_sr.o \
	operators/data_sender_shuffle_rc_sr.o \
	operators/data_sender_bcast_rc_sr.o \
	operators/data_recver_rc_sr.o \
	operators/data_sender_shuffle_rc_read.o \
	operators/data_sender_bcast_rc_read.o \
	operators/data_recver_rc_read.o \

endif

#
# UNIT TESTS
#

ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
UNIT_TESTS+= \
	unit_tests/testhdf5scan \
	unit_tests/queryhdf5 \

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
UNIT_TESTS+= \
	unit_tests/testindexhdf5 \
	unit_tests/queryindexhashjoin \

endif
endif

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
UNIT_TESTS += \
	unit_tests/testfastbit \

endif

UNIT_TESTS += \
	unit_tests/testschema \
	unit_tests/testhash \
	unit_tests/testparser \
	unit_tests/testshmopen \
	unit_tests/testloader \
	unit_tests/testtable \
	unit_tests/testcomparator \
	unit_tests/testhashtable \
	unit_tests/testmemmaptable \
	unit_tests/testaffinitizer \
	unit_tests/testparallelqueue \
	unit_tests/testpagesort \
	unit_tests/testpagebitonicsort \
	unit_tests/testmaxcontiguousregion \
	unit_tests/getnext \
	unit_tests/conjunctionevaluator \
	unit_tests/querythreadidprepend \
	unit_tests/querymap \
	unit_tests/querymapsequence \
	unit_tests/querydate \
	unit_tests/queryagg \
	unit_tests/queryagg_compositekey \
	unit_tests/queryaggsum \
	unit_tests/queryaggsum_global \
	unit_tests/queryhashjoin \
	unit_tests/queryhashjoincomposite \
	unit_tests/querysortmergejoin \
	unit_tests/querysortmergecartesianprod \
	unit_tests/querympsmjoin \
	unit_tests/querympsmpkfkjoin \
	unit_tests/querypartitionedmpsmpkfkjoin \
	unit_tests/querympsmpartitionedjoin \
	unit_tests/querympsmcartesianprod \
	unit_tests/querympsmcartesianprodtwogroups \
	unit_tests/querypreprejoin \
	unit_tests/querypreprejoinpkfk \
	unit_tests/querypreprejoinfkpk \
	unit_tests/querypreprejoincartesianprod \
	unit_tests/queryshuffle \
	unit_tests/querymemsegmentwriter \
	unit_tests/queryproject \
	unit_tests/querysort \
	unit_tests/querypartition \
	unit_tests/querygenerator \
	unit_tests/querymerge \

ifneq ($(findstring ENABLE_MPI,$(CPPFLAGS)),)
MPI_TESTS = unit_tests/test_mpi_shuffle \
						unit_tests/test_mpi_bcast \

endif

ifneq ($(findstring ENABLE_RDMA,$(CPPFLAGS)),)
RDMA_TESTS = unit_tests/test_rc_read_shuffle \
						 unit_tests/test_rc_read_bcast \
						 unit_tests/test_rc_sr_shuffle \
						 unit_tests/test_rc_sr_bcast \
						 unit_tests/test_ud_sr_shuffle \
						 unit_tests/test_ud_sr_bcast \

endif

DRIVERS = \
	drivers/executequery$(EXECSUFFIX) \
	drivers/executemultiplequery$(EXECSUFFIX) \
	drivers/sample_queries/q1/query1$(EXECSUFFIX) \
	drivers/enforcepageplacement_move$(EXECSUFFIX) \

ifneq ($(findstring ENABLE_MPI,$(CPPFLAGS)),)
DRIVERS += \
	drivers/mpiexecute$(EXECSUFFIX)
endif

# yes, two lines are necessary
define nl


endef

UNIT_TEST_OBJS = $(UNIT_TESTS)

ifneq ($(findstring ENABLE_MPI,$(CPPFLAGS)),)
MPI_TEST_OBJS = $(MPI_TESTS)

endif

ifneq ($(findstring ENABLE_RDMA,$(CPPFLAGS)),)
RDMA_TEST_OBJS = $(RDMA_TESTS)

endif

DRIVER_OBJS = $(DRIVERS)

all: dist clean tests $(DRIVER_OBJS)

tests: $(UNIT_TEST_OBJS) $(MPI_TEST_OBJS) $(RDMA_TEST_OBJS)
	unxz -fk unit_tests/data/ptf_small.h5.xz 
	$(foreach obj,$(MPI_TEST_OBJS),\
		export MV2_ENABLE_AFFINITY=0 && LD_LIBRARY_PATH=dist/lib/:$$LD_LIBRARY_PATH mpiexec -n 2 $(obj)$(nl)\
		)
	$(foreach obj,$(RDMA_TEST_OBJS),\
		LD_LIBRARY_PATH=dist/lib/:$$LD_LIBRARY_PATH $(obj)$(nl)\
		)
	$(foreach obj,$(UNIT_TEST_OBJS),\
		LD_LIBRARY_PATH=dist/lib/:$$LD_LIBRARY_PATH $(obj)$(nl)\
		)
	rm -f unit_tests/data/ptf_small.h5
	

clean:
	rm -f unit_tests/data/ptf_small.h5
	rm -f *.o
	rm -f visitors/*.o
	rm -f operators/*.o
	rm -f operators/loaders/*.o
	rm -f operators/loaders/sfmt/*.o
	rm -f util/*.o
	rm -f unit_tests/*.o
	rm -f $(UNIT_TEST_OBJS)
	rm -f $(MPI_TEST_OBJS)
	rm -f $(RDMA_TEST_OBJS)
	rm -f $(DRIVER_OBJS)
	rm -f util/timestamp.o

distclean: clean
	rm -rf dist
	rm -rf html
	rm -f tags

doc: Doxyfile
	doxygen

tags: 
	ctags -R --langmap=c++:+.inl --languages=c++ --exclude="dist/* externals/*"

drivers: $(DRIVER_OBJS)

util/timestamp.o:
	rm -f util/timestamp.cpp
	echo -n "const char* MakefileTimestampSignature=\"" >> util/timestamp.cpp
	echo -n Compiled on `date`,\  >> util/timestamp.cpp
	echo -n git HEAD is `git rev-parse HEAD`,\  >> util/timestamp.cpp
	echo -n using CXX=$(CXX),\  >> util/timestamp.cpp
	echo -n CXXFLAGS=$(CXXFLAGS),\  >> util/timestamp.cpp
	echo -n CPPFLAGS=$(CPPFLAGS),\  >> util/timestamp.cpp
	echo -n LDFLAGS=$(LDFLAGS),\  >> util/timestamp.cpp
	echo -n and LDLIBS=$(LDLIBS).\  >> util/timestamp.cpp
	echo -n Compiler version is:\ >> util/timestamp.cpp
	$(CXX) --version | head -1 | tr -d \\n >> util/timestamp.cpp
	echo -n .\  >> util/timestamp.cpp
	$(CXX) -Q --help=target | tr \\n \; | tr \\t \  | tr -s \  >> util/timestamp.cpp
	echo "\";" >> util/timestamp.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) util/timestamp.cpp -c -o $@
	rm -f util/timestamp.cpp

$(UNIT_TEST_OBJS): $(FILES)

$(MPI_TEST_OBJS): $(FILES)

$(RDMA_TEST_OBJS): $(FILES)

$(DRIVER_OBJS): $(FILES) util/timestamp.o 
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(subst $(EXECSUFFIX),,$@.cpp) $^ $(LDLIBS) -o $@

dist:
	./pre-init.sh
