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

/**
 * This program is to test generator_int and sortop.
 */

#include <iostream>
#include "libconfig.h++"

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"

//#define VERBOSE

using namespace std;
using namespace libconfig;

Query q;

IntGeneratorOp node1;
SortOp node2;

void compute() 
{
	q.threadInit();

	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

	CtLong minval = 0;

	assert(q.getOutSchema().getColumnType(0) == CT_LONG);

	while(result.first == Operator::Ready) 
	{
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		CtLong newval;

		while ( (tuple = it.next()) ) 
		{
			newval = q.getOutSchema().asLong(out->getTupleOffset(0), 0);
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			assert(minval <= newval);
			minval = newval;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

int main()
{
	Config cfg;

	cfg.getRoot().add("path", Setting::TypeString) = "/";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = 4096;

	// Init node1
	Setting& generatenode = cfg.getRoot().add("generator_int", Setting::TypeGroup);
	generatenode.add("ratio", Setting::TypeInt) = 4;
	generatenode.add("sizeinb", Setting::TypeInt) = 
		static_cast<int>(1024 * 1024 * (sizeof(CtLong) * 3));

	// Init node2
	Setting& sortnode = cfg.getRoot().add("sort", Setting::TypeGroup);
	sortnode.add("attr", Setting::TypeInt) = 0;
	Setting& tuplenum = sortnode.add("maxtuples", Setting::TypeList);
	tuplenum.add(Setting::TypeString) = "1 M";

	// build plan tree
	q.tree = &node2;
	node2.nextOp = &node1;

	// initialize each node
	node1.init(cfg, generatenode);
	node2.init(cfg, sortnode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	q.destroynofree();

	return 0;
}
