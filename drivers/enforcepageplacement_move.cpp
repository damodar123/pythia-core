/*
 * Copyright 2016, Pythia authors (see AUTHORS file).
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

#include <iostream>
#include <unistd.h>
using namespace std;

#ifdef ENABLE_NUMA
#include <numaif.h>
#include <syscall.h>

#include "../rdtsc.h"
#include "../hash.h"

#include "../schema.h"
#include "../hash.h"
#include "../util/buffer.h"

#include "../util/affinitizer.h"
#include "../util/numaallocate.h"
#include "../util/numaasserts.h"
#include "../operators/loaders/table.h"
#include "../operators/loaders/loader.h"
#include "../operators/loaders/parser.h"

void affinitize(int offset);

/**
 * Tests whether all tuples in table came from the same NUMA node, and prints
 * NUMA node at the end. Side-effects: Resets table cursor.
 */
void testallsamenuma(Table& table)
{
	table.reset();

	vector<unsigned int> location(16, 0);

	for(TupleBuffer* t = NULL; (t = table.readNext()); )
	{
		TupleBuffer::Iterator it = t->createIterator();
		for (void* tuple = NULL; (tuple = it.next()); )
		{
			int curnuma = numafromaddress(tuple);
			location.at(curnuma)++;
		}
	}

	for (unsigned int i=0; i < location.size(); ++i)
	{
		if (location[i] != 0)
			cout << location[i] << " pages were located on NUMA node " 
				<< i << "." << endl;
	}
}

void reportnumaness(const string& filepattern)
{
	MemMappedTable table;
	Schema schema;
	schema.add(CT_CHAR, getpagesize());
	table.init(&schema);
	assert(Table::LOAD_OK == table.load(filepattern, "", Table::SilentLoad, Table::SortFiles));

	testallsamenuma(table);

	table.close();
}

int sanitycheck(char c)
{
	if (c >= '0' && c <= '9')
	{
		return c - '0';
	}
	return -1;
}

void dorealwork(int argc, char** argv)
{
	vector<int> targetnodes;
	vector<int> resultcodes;
	vector<void*> addresses;

	int targetnode = -1;
	unsigned int movedsofar = 0;

	if (argc == 2)
	{
		// Guess numa node by parsing string.
		string filepattern(argv[1]); 
		size_t idx = filepattern.find("numa");
		if (idx != string::npos)
			targetnode = sanitycheck(filepattern[idx+4]);
	}
	else if (argc > 2)
	{
		// argv[2] is the numa node
		char c = argv[2][0];
		targetnode = sanitycheck(c);
	}

	if (argc <= 1 || targetnode == -1)
	{
		cout << "ERROR IN PARAMETERS; DID NOTHING." << endl;
		return;
	}

	string filepattern(argv[1]); 
	filepattern += "*";

	MemMappedTable table;
	Schema schema;
	schema.add(CT_CHAR, getpagesize());
	table.init(&schema);
	assert(Table::LOAD_OK == table.load(filepattern, "", Table::SilentLoad, Table::SortFiles));
	
	for(TupleBuffer* t = NULL; (t = table.readNext()); )
	{
		TupleBuffer::Iterator it = t->createIterator();
		for (void* tuple = NULL; (tuple = it.next()); )
		{
			if (numafromaddress(tuple) == targetnode)
				continue; 

			targetnodes.push_back(targetnode);
			resultcodes.push_back(0);
			addresses.push_back(tuple);

			++movedsofar;
		}
	}

	cout << "Found " << movedsofar << " pages to move to NUMA node " << targetnode << "." << endl;

	assert(addresses.size() == targetnodes.size());
	assert(addresses.size() == resultcodes.size());
	if(0 != move_pages(0, addresses.size(), &addresses[0], &targetnodes[0], &resultcodes[0], MPOL_MF_MOVE))
	{
		perror(" FATAL ERROR");
		assert(0 == move_pages(0, addresses.size(), &addresses[0], &targetnodes[0], &resultcodes[0], MPOL_MF_MOVE));
	}

	table.close();

	cout << "Tried to move " << movedsofar << " pages to NUMA node " << targetnode << "." << endl;

}

void dowork(MemMappedTable& table, Schema& s, int argc, char** argv)
{
	dorealwork(argc, argv);
}

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		cout << "ERROR: Specify file pattern to load." << endl;
		return 2;
	}

	string filepattern(argv[1]);
	filepattern += "*";

	affinitize(MemoryTopology.topology.at(0).at(0).at(0).at(0));
	cout << "Current thread is affinitized to NUMA node 0." << endl;

	cout << "Loading from \"" << filepattern << "\"..." << endl;

	MemMappedTable table;
	Schema schema;
	schema.add(CT_INTEGER);
	table.init(&schema);
	assert(Table::LOAD_OK == table.load(filepattern, "", Table::SilentLoad, Table::SortFiles));

	dowork(table, schema, argc, argv);

	table.close();

	reportnumaness(filepattern);

	return 0;
}

#else // ENABLE_NUMA

int main()
{
	cout << "Compiled without NUMA support, exiting." << endl;
}

#endif
