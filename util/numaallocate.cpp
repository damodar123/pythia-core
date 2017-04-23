/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifdef ENABLE_NUMA
#include <numaif.h>
#endif

/* Define to protect mbind() in critical segment. This is a workaround for
 * this RHEL6 bug:
 *   http://lkml.org/lkml/2011/6/27/233
 *   https://bugzilla.redhat.com/show_bug.cgi?id=727700
 *
 * #define MBIND_BUG_WORKAROUND
 */

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cassert>
#include <execinfo.h>
#include <deque>
#include <set>

#include "numaasserts.h"
#include "atomics.h"
#include "../lock.h"
#include "topology.h"

// const size_t HUGETLBSIZE = 2uLL * 1024 * 1024;  // 2MB pages
const size_t HUGETLBSIZE = 1uLL * 1024 * 1024 * 1024;  // 1GB pages

struct AllocHugeT
{
	AllocHugeT(void* location, size_t size)
		: location(location), size(size)
	{ }

	AllocHugeT()
		: location(0), size(0)
	{ }

	void* location;
	size_t size;

	inline bool operator< (const AllocHugeT& rhs) const
	{
		return location < rhs.location;
	}

	inline bool operator== (const AllocHugeT& rhs) const
	{
		return location == rhs.location;
	}

};

std::set<AllocHugeT> AllocHugePages;
typedef std::set<AllocHugeT>::iterator AllocHugePagesIteratorT;

Lock NumaAllocLock;

size_t TotalBytesAllocated = 0;
const int MAX_NUMA = 8;

#ifdef STATS_ALLOCATE
#include <map>
#include <string>
#include <utility>
#include "../query.h"
#include <iostream>
#include <sstream>
#include <iomanip>
using std::map;
using std::pair;
using std::string;
using std::make_pair;

Lock AllocStatsLock;
Lock AllocTimeLock;

struct AllocT
{
	AllocT(void* source, string tag, char numareq, char numaacq)
		: source(source), tag(tag), numareq(numareq), numaacq(numaacq)
	{ }

	AllocT()
		: source(0), numareq(-3), numaacq(-3)
	{ }

	inline bool operator< (const AllocT& rhs) const
	{
		if (source != rhs.source)
			return source < rhs.source;
		if (tag != rhs.tag)
			return tag < rhs.tag;
		if (numareq != rhs.numareq)
			return numareq < rhs.numareq;
		if (numaacq != rhs.numaacq)
			return numaacq < rhs.numaacq;
		return false;
	}

	inline bool operator== (const AllocT& rhs) const
	{
		return (source == rhs.source) && (tag == rhs.tag) 
			&& (numareq == rhs.numareq) && (numaacq == rhs.numaacq);
	}

	void* source;	///< What should be "charged" for this allocation.
	string tag;		///< Human-readable tag, printed via dbgPrintAllocations.
	char numareq;	///< Requested NUMA node.
	char numaacq;	///< NUMA node memory was acquired on.
};

typedef map<AllocT, size_t> AllocStatT;
AllocStatT AllocStats;

struct AllocTimeT
{
	timespec alloctime;
	size_t allocsize;
	char flag;
};
deque<AllocTimeT> AllocTime;

void innerupdatestats(void* source, const char tag[4], char numareq, char numaacq, size_t allocsize)
{
	// Keep allocation sizes in global static "allocstats" object
	string tagstr(tag, 4);
	AllocStatT::key_type key(source, tagstr, numareq, numaacq);

	AllocStatsLock.lock();
	AllocStats[key] += allocsize;
	AllocStatsLock.unlock();
}

void updatetimestats(char flag, size_t allocsize)
{
	AllocTimeT temp;
	clock_gettime(CLOCK_MONOTONIC, &temp.alloctime);
	temp.allocsize = allocsize;
	temp.flag = flag;

	AllocTimeLock.lock();
	AllocTime.push_back(temp);
	AllocTimeLock.unlock();
}

// from visitors/prettyprint.cpp
string addcommas_str(const string& input);

template <typename T>
string addcommas(const T& input)
{
	std::ostringstream ss;
	ss << input;
	return addcommas_str(ss.str());
}

void dbgPrintAllocations(Query& q)
{
	using namespace std;
	cout << "Depth" << " " << " Tag" << " " << "NumaReq" << " " 
		<< "NumaAcq" << " " << "PeakAlloc(bytes)" << endl;
	for (AllocStatT::const_iterator it = AllocStats.begin();
			it != AllocStats.end();
			++it)
	{
		const AllocT& key = it->first;
		cout << setfill(' ') << setw(5) << q.getOperatorDepth((Operator*)key.source) << " ";
		cout << setfill(' ') << setw(4) << key.tag << " ";

		if (key.numareq == -1) 
		{ 
			cout << "      L ";
		}
		else
		{
			cout << setfill(' ') << setw(7) << (int)key.numareq << " ";
		}

		if (key.numaacq == -1) 
		{ 
			cout << "      L ";
		}
		else
		{
			cout << setfill(' ') << setw(7) << (int)key.numaacq << " ";
		}

		cout << setfill(' ') << setw(16) << addcommas(it->second) << endl;
	}
}

void dbgPrintTimeAlloc()
{
	using namespace std;
	cout << "Time" << " ";
	cout << setfill(' ') << setw(19) << " type " << " Alloc(bytes)" << endl;
	// iterate over AllocTime, print each entry
	for (deque<AllocTimeT>::const_iterator it = AllocTime.begin();
			it != AllocTime.end();
			++it)
	{
		cout << setfill(' ') << setprecision(17) << it->alloctime.tv_sec + it->alloctime.tv_nsec / 1000000000.0  << " ";
		cout << setfill(' ') << setw(5) << it->flag << " ";
		cout << setfill(' ') << setw(13) << it->allocsize << endl;
	}

}

void dbgStartTimeAlloc()
{
	updatetimestats('+',0);
}

void dbgStopTimeAlloc()
{
	updatetimestats('-',0);
}
#endif

void updatestats(void* source, const char tag[4], char numareq, char numaacq, size_t allocsize)
{
#ifdef STATS_ALLOCATE
	innerupdatestats(source, tag, numareq, numaacq, allocsize);
#endif
	atomic_increment(&TotalBytesAllocated, allocsize);
}

struct AllocHeader
{
	void* calleraddress;
	char tag[4];
	bool mmapalloc;
	size_t allocsize;

	// Bitonic sort needs 16-byte aligned values. Hacking it to do so.
	//
	char padding[8];
};

void populateHeader(void* dest, const char tag[4], bool mmapalloc, size_t allocsize)
{
	AllocHeader* d = (AllocHeader*) dest;
	void* retaddbuf[3];

	assert(3 == backtrace(retaddbuf, 3));

	d->calleraddress = retaddbuf[2];
	d->tag[0] = tag[0];
	d->tag[1] = tag[1];
	d->tag[2] = tag[2];
	d->tag[3] = tag[3];
	d->mmapalloc = mmapalloc;
	d->allocsize = allocsize;
}

struct LookasideHeader
{
	volatile void* free;
	size_t maxsize;
};

struct Lookaside
{
	static void* arena[MAX_NUMA];
};

/**
 * Initial population of lookasides.
 *
 * WARNING: One cannot touch global objects yet, as lookaside_init_alloc() is
 * executed before main() and other global objects may not have been
 * constructed yet.
 */
void* lookaside_init_alloc(size_t allocsize, int node)
{
	void* memory = NULL;
	assert(allocsize > sizeof(LookasideHeader));
	assert(node != -1);

#ifndef ENABLE_NUMA
	assert(EnumerateTopology().totalnumanodes() == 1);
#endif

	// If NUMA node does not exist, don't allocate anything.
	//
	if (node >= EnumerateTopology().totalnumanodes())
	{
		return NULL;
	}

	int sethugetlb = 0;
	if ((allocsize & (HUGETLBSIZE-1)) == 0)
	{
		sethugetlb = MAP_HUGETLB;
	}

	memory = mmap(NULL, allocsize, 
			PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | sethugetlb, 
			-1, 0);
	if ((memory == MAP_FAILED) && (sethugetlb != 0))
	{
		// HUGETLB allocation unsuccessful, reverting to regular code path.
		//
		sethugetlb = 0;
		memory = mmap(NULL, allocsize, 
				PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | sethugetlb, 
				-1, 0);
	}
	assert(memory != MAP_FAILED);

#ifdef ENABLE_NUMA
	int retries = 1024;
	unsigned long numanodemask = 1uLL << node;
	unsigned long maxnode = sizeof(numanodemask);
	assert(static_cast<int>(maxnode) > node);
	assert(node >= 0);
	int res = 0;
	do
	{
		res = mbind(memory, allocsize, 
				MPOL_BIND, &numanodemask, maxnode, 
				MPOL_MF_STRICT | MPOL_MF_MOVE); 
	}
	while ((res != 0) && ((--retries) != 0));
#endif

	LookasideHeader* lh = (LookasideHeader*)memory;
	lh->free = &lh[1];
	lh->maxsize = allocsize - sizeof(LookasideHeader);

	assertaddressonnuma(memory, node);

	return memory;
}

void* Lookaside::arena[MAX_NUMA] = {
	lookaside_init_alloc(1uLL*1024*1024*1024, 0),
	lookaside_init_alloc(1uLL*1024*1024*1024, 1),
	lookaside_init_alloc(1uLL*1024*1024*1024, 2),
	lookaside_init_alloc(1uLL*1024*1024*1024, 3),
	lookaside_init_alloc(1uLL*1024*1024*1024, 4),
	lookaside_init_alloc(1uLL*1024*1024*1024, 5),
	lookaside_init_alloc(1uLL*1024*1024*1024, 6),
	lookaside_init_alloc(1uLL*1024*1024*1024, 7)
};
static_assert(MAX_NUMA == 8);	// need to add functions above, otherwise.

/**
 * Function does allocation via mmap().
 *
 * node = -1 triggers local allocation
 * node = -2 triggers striped (interleaved) allocation across all NUMA nodes
 *
 * If allocsize is a multiple of HUGETLBSIZE, the allocation is
 * attempted with the huge pages flag MAP_HUGETLB set. If allocation
 * is successful, then no AllocHeader is prepended and instead memory is
 * tracked in an STL set, AllocHugePages. If allocation is unsuccessful,
 * the code reverts to the regular code path and allocates without the
 * MAP_HUGETLB flag and with the usual AllocHeader structure.
 *
 */
void* slowallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
#ifndef NULL
#define NULL 0
#endif
	void* memory = NULL;

	unsigned long numanodemask = 0;
	if (node == -2)
	{
		for (unsigned int nd = 0; nd < MemoryTopology.topology.size(); ++nd)
		{
			numanodemask |= (1uLL << nd);
		}
	}
	else
	{
		numanodemask = 1uLL << node;
	}
	unsigned long maxnode = sizeof(numanodemask);
	assert(static_cast<int>(maxnode) > node);

#ifdef MBIND_BUG_WORKAROUND
	// Locking as a stopgap fix to a known kernel bug when mbind() is called
	// concurrently: 
	//   http://lkml.org/lkml/2011/6/27/233
	//
	NumaAllocLock.lock();
#endif

	int sethugetlb = 0;
	if ((allocsize & (HUGETLBSIZE-1)) == 0)
	{
		sethugetlb = MAP_HUGETLB;
	}
	else
	{
		allocsize += sizeof(AllocHeader);
	}

	memory = mmap(NULL, allocsize, 
			PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | sethugetlb, 
			-1, 0);
	if ((memory == MAP_FAILED) && (sethugetlb != 0))
	{
		// HUGETLB allocation unsuccessful, reverting to regular code path.
		//
		sethugetlb = 0;
		allocsize += sizeof(AllocHeader);
		memory = mmap(NULL, allocsize, 
				PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | sethugetlb, 
				-1, 0);
	}
	assert(memory != MAP_FAILED);

#ifdef ENABLE_NUMA
	int res = 0;
	int retries = 1024;
	do
	{
		if (node == -1)
		{
			res = mbind(memory, allocsize, 
					MPOL_PREFERRED, NULL, 0, 
					MPOL_MF_STRICT | MPOL_MF_MOVE); 
		}
		else if (node == -2)
		{
			res = mbind(memory, allocsize, 
					MPOL_INTERLEAVE, &numanodemask, maxnode, 
					MPOL_MF_STRICT | MPOL_MF_MOVE); 
		}
		else
		{
			res = mbind(memory, allocsize, 
					MPOL_BIND, &numanodemask, maxnode, 
					MPOL_MF_STRICT | MPOL_MF_MOVE); 
		}
	}
	while ((res != 0) && ((--retries) != 0));
	assert(res==0);
#endif

#ifdef MBIND_BUG_WORKAROUND
	NumaAllocLock.unlock();
#endif

	if (sethugetlb == 0)
	{
		populateHeader(memory, tag, true, allocsize);
		updatestats(source, tag, node, node, allocsize);

		return ((char*)memory) + sizeof(AllocHeader);
	}
	else
	{
		assert (sethugetlb == MAP_HUGETLB);
		updatestats(source, tag, node, node, allocsize);

		// Keep track of huge page, and assert that the huge page does not 
		// already exist in the set.
		AllocHugeT aht(memory, allocsize);
		NumaAllocLock.lock();
		std::pair<AllocHugePagesIteratorT, bool> ret 
			= AllocHugePages.insert(aht);
		NumaAllocLock.unlock();
		assert(ret.second == true);

		return memory;
	}
}

/**
 * Function looks up for space in the local lookaside, and returns NULL if
 * there's no space.
 * If node == -1 or node == -2, local allocation is performed.
 */
void* fastallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
	int nodeacq = -3;

	allocsize += sizeof(AllocHeader);

	// Round up to the next 64-byte multiple to maintain alignement. 
	// Then add 64 bytes to elimnate false sharing.
	//
	allocsize = (((allocsize+64) / 64) * 64) + 64;

	if (node < 0)
	{
		nodeacq = localnumanode();
	}
	else
	{
		nodeacq = node;
	}

	assert(nodeacq >= 0);
	assert(nodeacq < MAX_NUMA);

	LookasideHeader* lh = (LookasideHeader*) Lookaside::arena[nodeacq];
	assert(lh != NULL);
	void* data = &lh[1];
	assert(data <= lh->free);

	void* oldval;
	void* newval;
	void** val = (void**)&(lh->free);

	newval = *val;
	do {
		if (lh->free > ((char*)data + lh->maxsize - allocsize))
		{
			// Not enough space in arena, return nothing.
			//
			return NULL;
		}

		oldval = newval;
		newval = (char*)oldval + allocsize;
		newval = atomic_compare_and_swap(val, oldval, newval);
	} while (newval != oldval);

	populateHeader(newval, tag, false, allocsize);
	updatestats(source, tag, node, nodeacq, allocsize);

	return ((char*)newval) + sizeof(AllocHeader);
}

/** 
 * NUMA-aware allocator main entry point.
 * If node is -1, do local allocation, else allocate on specified node.
 * If node is -2, do local allocation if small, else stripe on all NUMA nodes.
 * Tag and Source are user-defined values that get printed via dbgPrintAllocations.
 */
void* numaallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
#ifndef ENABLE_NUMA
	assert(node <= 0);
#endif

	void* memory = NULL;

	// If more than 16M, go to slow allocator to avoid pollution of arena.
	//
	if (allocsize <= 16uLL*1024*1024)
	{
		memory = fastallocate_onnode(tag, allocsize, node, source);
	}

	if (memory == NULL)
	{
		memory = slowallocate_onnode(tag, allocsize, node, source);
	}
	assert((((unsigned long long)memory) & 0x7) == 0);

	// Assert that NUMA-ness has been respected.
	//
	if (node == -1)
	{
		assertaddresslocal(memory);
	}
	else if (node >= 0)
	{
		assertaddressonnuma(memory, node);
	}

	return memory;
}

void* numaallocate_local(const char tag[4], size_t allocsize, void* source)
{
	return numaallocate_onnode(tag, allocsize, -1, source);
}

void numadeallocate(void* space)
{
	int res = 0;

	// First find if \a space has been allocated as a huge page, which will not
	// contain AllocHeader information.
	//
	NumaAllocLock.lock();
	AllocHugePagesIteratorT it = AllocHugePages.find(AllocHugeT(space, 0));
	if (it != AllocHugePages.end())
	{
		assert(it->location == space);
		assert(it->size != 0);

		size_t allocsz = it->size;
		AllocHugePages.erase(it);

		NumaAllocLock.unlock();

		res = munmap(space, allocsz);
		assert(res==0);

		return;
	}
	NumaAllocLock.unlock();

	// Not a huge page, proceed to access AllocHeader information.
	//
	AllocHeader* d = (AllocHeader*) (((char*)space) - sizeof(AllocHeader));

	if (d->mmapalloc)
	{
		// If allocated via mmap(), deallocate.
		//
		res = munmap(d, d->allocsize);
		assert(res==0);
	}
	else
	{
		// Memory was borrowed from lookaside. Do nothing. 
		//
		// One day this will mark slots as deleted, and trigger compaction.
		//
		;
	}
}

void accountfordealloc(size_t allocsize)
{
#ifdef STATS_ALLOCATE
	updatetimestats('-', allocsize);
#endif
}

void accountforalloc(size_t allocsize)
{
#ifdef STATS_ALLOCATE
	updatetimestats('+', allocsize);
#endif
}
