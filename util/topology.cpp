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

#include <vector>
#include <cassert>
using std::vector;
#include "topology.h"

#include <iostream>
using std::cout;
using std::endl;


/**
 * Returns a vector of CPUs for the particular NUMA \a node.
 */
vector<unsigned short> TopologyT::cpusfornode(const int node)
{
	vector<unsigned short> ret;

	vector<vector<vector<unsigned short> > > & numanode = topology.at(node);
	for (unsigned int i=0; i<numanode.size(); ++i)
	{
		vector<vector<unsigned short> > & socketnode = numanode[i];
		for (unsigned int j=0; j<socketnode.size(); ++j)
		{
			vector<unsigned short> & corenode = socketnode[j];
			for (unsigned int k=0; k<corenode.size(); ++k)
			{
				ret.push_back(corenode[k]);
			}
		}
	}

	return ret;
}

/**
 * Find NUMA node given cpu ID.
 */
int TopologyT::numaforcpu(const int cpu)
{
	for (unsigned int numa=0; numa<topology.size(); numa++)
	{
	vector<vector<vector<unsigned short> > > & numanode = topology[numa];
		for (unsigned int i=0; i<numanode.size(); ++i)
		{
			vector<vector<unsigned short> > & socketnode = numanode[i];
			for (unsigned int j=0; j<socketnode.size(); ++j)
			{
				vector<unsigned short> & corenode = socketnode[j];
				for (unsigned int k=0; k<corenode.size(); ++k)
				{
					if (cpu == corenode[k])
					{
						return numa;
					}
				}
			}
		}
	}

	// NUMA node does not appear in mapping.
	//
	assert(false);
}

int TopologyT::totalsockets()
{
	int ret = 0;
	for (unsigned int numa=0; numa<topology.size(); numa++)
	{
		ret += topology[numa].size();
	}
	return ret;
}

int TopologyT::totalcores()
{
	int ret = 0;
	for (unsigned int numa=0; numa<topology.size(); numa++)
	{
		vector<vector<vector<unsigned short> > > & numanode = topology[numa];
		for (unsigned int i=0; i<numanode.size(); ++i)
		{
			ret += numanode[i].size();
		}
	}
	return ret;
}

int TopologyT::totalthreads()
{
	int ret = 0;
	for (unsigned int numa=0; numa<topology.size(); numa++)
	{
		vector<vector<vector<unsigned short> > > & numanode = topology[numa];
		for (unsigned int i=0; i<numanode.size(); ++i)
		{
			vector<vector<unsigned short> > & socketnode = numanode[i];
			for (unsigned int j=0; j<socketnode.size(); ++j)
			{
				ret += socketnode[j].size();
			}
		}
	}
	return ret;
}

void dbgPrintTopology()
{
	TopologyT::InternalTopologyT& topology = MemoryTopology.topology;

	for (unsigned int numa=0; numa<topology.size(); numa++)
	{
		vector<vector<vector<unsigned short> > > & numanode = topology[numa];
		for (unsigned int socket=0; socket<numanode.size(); ++socket)
		{
			vector<vector<unsigned short> > & socketnode = numanode[socket];
			for (unsigned int core=0; core<socketnode.size(); ++core)
			{
				vector<unsigned short> & corenode = socketnode[core];
				for (unsigned int thread=0; thread<corenode.size(); ++thread)
				{
					cout << "[" 
						<< numa << ", " 
						<< socket << ", " 
						<< core << ", " 
						<< thread << "] = CPU" 
						<< corenode[thread] << endl;
				}
			}
		}
	}
}
