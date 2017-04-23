CXX=g++
CXXFLAGS+=-m64

###########################################################
# Controls sanity-checking level. Enable for debug builds. 
# DEBUG2 triggers severe performance penalties.
#
#override CPPFLAGS+=-DDEBUG -DDEBUG2

###########################################################
# Controls whether to collect and display memory allocation
# statistics.
#
override CPPFLAGS+=-DSTATS_ALLOCATE

###########################################################
# Compile with NUMΑ-awareness extensions?
#
#override CPPFLAGS+=-DENABLE_NUMA

###########################################################
# Enables bitonic sort. This is an experimental feature. 
# The algorithm that has been implemented is data-size and 
# data-width specific and not generally usable. Bitonic 
# sort needs SSE 4.2 support from the CPU.
#
#override CPPFLAGS+=-DBITONIC_SORT

###########################################################
# Controls compiling of HDF5-specific operators
#
#override CPPFLAGS+=-DENABLE_HDF5

###########################################################
# FastBit source code is not included
# As of Jan 2014, Pythia v0.1 uses pre-release functionality
#
#override CPPFLAGS+=-DENABLE_FASTBIT

###########################################################
# Compile with RDMA support?
#
#override CPPFLAGS+=-DENABLE_RDMA

###########################################################
# Compile with MPI support?
#
#override CPPFLAGS+=-DENABLE_MPI

