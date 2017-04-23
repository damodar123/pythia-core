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

#include "operators.h"
#include "operators_priv.h"

void IntGeneratorOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	ZeroInputOp::init(root, node);

	// Populate output schema.
	//
	schema.add(CT_LONG);
	schema.add(CT_LONG);
	schema.add(CT_LONG);
	
	// Read parameters, make sure they make sense.
	//
	ratio = node["ratio"];

	if (node.exists("offsetinM"))
	{
		int offsetinM = 0;
		offsetinM = node["offsetinM"];
		offset = offsetinM * 1024ull * 1024ull; 
	}
	else
	{
		offset = 0;
	}

	int tuples;

	if (node.exists("sizeinmb"))
	{
		tuples = node["sizeinmb"];
		totaltuples = tuples * 1024ull * 1024ull / schema.getTupleSize();
	}
	else
	{
		tuples = node["sizeinb"];
		totaltuples = tuples / schema.getTupleSize();
	}

	// Populate private data structures.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		scratchspace.push_back(NULL);
		output.push_back(NULL);
		producedsofar.push_back(0);
		randintgen.push_back(NULL);
	}
}

void IntGeneratorOp::threadInit(unsigned short threadid)
{
	static const char dummy[296] = "The past performance does not guarantee future results. We provide self-directed users with data services, and do not make recommendations or offer legal or other advice. You alone are responsible for evaluating the merits and risks associated with the use of our systems, services or products. ";
	assert(schema.getTupleSize() < sizeof(dummy));

	scratchspace.at(threadid) = new char[schema.getTupleSize()];
	dbgassert(scratchspace.at(threadid) != NULL);
	schema.copyTuple(scratchspace.at(threadid), dummy);

	output.at(threadid) = new Page(buffsize, schema.getTupleSize(), this);
	producedsofar.at(threadid) = 0;
	randintgen.at(threadid) = new ReentrantRandom();
	randintgen.at(threadid)->init(threadid);
}

void* IntGeneratorOp::produceOne(unsigned short threadid)
{
	dbgassert(scratchspace.at(threadid) != NULL);
	void* tuple = scratchspace[threadid];

	CtLong* curval = &producedsofar[threadid];

	if (*curval >= totaltuples)
		return NULL;

	static_assert(RAND_MAX == 0x7FFFFFFFuLL);
	CtLong randlong = ((CtLong) randintgen.at(threadid)->next() << 31)
		| (randintgen.at(threadid)->next() & 0x7FFFFFFFuLL);
	schema.writeData(tuple, 0, &randlong);

	CtLong tempattr = ((*curval) + offset + ratio) / ratio;
	schema.writeData(tuple, 1, &tempattr);

	++(*curval);
	CtLong tempind = (*curval) + offset;
	schema.writeData(tuple, 2, &tempind);

	return tuple;
}

Operator::GetNextResultT IntGeneratorOp::getNext(unsigned short threadid)
{
	dbgassert(output.at(threadid) != NULL);
	Page* out = output[threadid];
	out->clear();

	while (out->canStoreTuple())
	{
		void* tuple = produceOne(threadid);
		if (tuple == NULL)
			return make_pair(Finished, out);

		void* target = out->allocateTuple();
		dbgassert(target != NULL);

		schema.copyTuple(target, tuple);
	}

	return make_pair(Ready, out);
}

void IntGeneratorOp::threadClose(unsigned short threadid)
{
	if (output.at(threadid))
	{
		delete output.at(threadid);
	}
	output.at(threadid) = NULL;

	if (scratchspace.at(threadid))
	{
		delete[] scratchspace.at(threadid);
	}
	scratchspace.at(threadid) = NULL;

	producedsofar.at(threadid) = 0;

	if (randintgen.at(threadid))
	{
		delete randintgen.at(threadid);
	}
	randintgen.at(threadid) = NULL;
}
