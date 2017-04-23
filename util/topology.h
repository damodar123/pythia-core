/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
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

#ifndef __MYTOPOLOGY__
#define __MYTOPOLOGY__

#include <vector>

struct TopologyT
{
	typedef std::vector<std::vector<std::vector<std::vector<unsigned short> > > > InternalTopologyT;

	TopologyT(InternalTopologyT::size_type n, InternalTopologyT::value_type v)
		: topology(n, v)
	{ }

	/**
	 * Returns a vector of CPUs for the particular NUMA \a node.
	 */
	std::vector<unsigned short> cpusfornode(const int node);

	/**
	 * Find NUMA node given cpu ID.
	 */
	int numaforcpu(const int cpu);

	/**
	 * Return number of NUMA nodes in system.
	 */
	int totalnumanodes()
	{
		return topology.size();
	}

	/**
	 * Return number of sockets in system.
	 */
	int totalsockets();

	/**
	 * Return number of cores in system.
	 */
	int totalcores();

	/**
	 * Return number of threads in system.
	 */
	int totalthreads();

	InternalTopologyT topology;
};

extern TopologyT MemoryTopology;
TopologyT EnumerateTopology();

#endif 
