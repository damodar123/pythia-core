
/*
 * Copyright 2012, Pythia authors (see AUTHORS file).
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
#include "../util/numaallocate.h"
#include <iostream>
#include <sstream>

using std::make_pair;

/**
* Function accepts a libconfig node and parses either a number of a string,
* and returns a number.
*
* Example 1:
*    numericalize("1024");
* This would return 1024.
*
* Example 2:
*    numericalize("1 G");
* This would return 1073741824 (which is 1024**3).
*
*/
unsigned long long numericalize(const string& s)
{
	unsigned long long ret = 1;

	char suffix;
	unsigned long long value;
	istringstream temp(s);
	temp >> value;
	temp >> suffix;

	switch(suffix)
	{
		case 'G':
		case 'g':
			ret *= 1024;
		case 'M':
		case 'm':
			ret *= 1024;
		case 'K':
		case 'k':
			ret *= 1024;
		default:
			break;
	}

	ret *= value;

	return ret;
}

void 
SortOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	// Attribute to sort and partition on, starting from 0. Must be numeric.
	//
	attribute = node["attr"];
	switch(schema.getColumnType(attribute))
	{
		// Currently only support numeric data types.
		case CT_INTEGER:
		case CT_LONG:
		case CT_DATE:
		case CT_DECIMAL:
			break;
		default:
			throw NotYetImplemented();
	};

	// Compute max size of staging area used for sorting inputs.
	//
	libconfig::Setting& maxtuplecfg = node["maxtuples"];

	assert(maxtuplecfg.isAggregate());
	for (int i=0; i<maxtuplecfg.getLength(); ++i)
	{
		string temp = maxtuplecfg[i];
		maxtuples.push_back(numericalize(temp));
	}
	
	// Create state, output and build/probe staging areas.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		datapage.push_back(NULL);
		state.push_back(NULL);
		gentuples.push_back(0ull);
	}
}


void SortOp::threadInit(unsigned short threadid)
{
	void* space;

	const unsigned int tuplesize = schema.getTupleSize();

	state[threadid] = new State();

	space = numaallocate_local("SrtI", sizeof(Page), this);
	datapage[threadid] = 
		new (space) Page(maxtuples[threadid]*tuplesize, tuplesize, this);
	
	output[threadid] = numaallocate_local("SrtO", sizeof(Page), this);
}

void SortOp::threadClose(unsigned short threadid)
{
	dbgassert(output.at(threadid) != NULL);
	dbgassert(datapage.at(threadid) != NULL);

	if (output[threadid]) 
	{
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;

	if (datapage[threadid])
	{
		numadeallocate(datapage[threadid]);
	}
	datapage[threadid] = NULL;

}

// The following functions are reused in join.cpp, so hiding from global
// namespace here.
//
namespace {

/**
 * Copies all tuples from source operator \a op into staging area \a page.
 * Assumes operator has scan-started successfully for this threadid.
 * Error handling is non-existant, asserts if anything is not expected.
 */
void copySourceIntoPage(Operator* op, Operator::Page* page, unsigned short threadid)
{
	Operator::GetNextResultT result;
	result.first = Operator::Ready;

	while (result.first == Operator::Ready)
	{
		result = op->getNext(threadid);
		assert(result.first != Operator::Error);

		void* datastart = result.second->getTupleOffset(0);

		if (datastart == 0)
			continue;

		unsigned int datasize = result.second->getUsedSpace();
		void* space = page->allocate(datasize);
		assert(space != NULL);
		memcpy(space, datastart, datasize);
	}

	assert(result.first == Operator::Finished);
}

void verifysorted(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	void* tup1 = 0;
	void* tup2 = 0;
	Operator::Page::Iterator it = page->createIterator();
	Comparator comp = Schema::createComparator(
			schema, joinattr,
			schema, joinattr,
			Comparator::LessEqual);

	tup1 = it.next();
	if (tup1 == NULL)
		return;

	while ( (tup2 = it.next()) )
	{
		assert(comp.eval(tup1, tup2) == true);
		tup1=tup2;
	}
}

/**
 * Sorts all tuples in given page.
 */
void sortAllInPage(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	unsigned int keyoffset = (unsigned long long)schema.calcOffset(0, joinattr);

	switch(schema.getColumnType(joinattr))
	{
		case CT_INTEGER:
			page->sort<CtInt>(keyoffset);
			break;
		case CT_LONG:
		case CT_DATE:
			page->sort<CtLong>(keyoffset);
			break;
		case CT_DECIMAL:
			page->sort<CtDecimal>(keyoffset);
			break;
		default:
			throw NotYetImplemented();
	}
}

};

Operator::ResultCode SortOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	dbgassert(output.at(threadid) != NULL);
	dbgassert(datapage.at(threadid) != NULL);
	dbgassert(state.at(threadid)->remainingdatapos == State::INVALID_OFFSET);

	state[threadid]->remainingdatapos = 0;

	// Copy chunks into staging area input[threadid].
	//
	assert(Ready == nextOp->scanStart(threadid, indexdatapage, indexdataschema));
	copySourceIntoPage(nextOp, datapage[threadid], threadid);
	assert(Ready == nextOp->scanStop(threadid));

	gentuples[threadid] = datapage[threadid]->getNumTuples();
	
	// Sort build side.
	//
	sortAllInPage(datapage[threadid], nextOp->getOutSchema(), attribute);
#ifdef DEBUG
	verifysorted(datapage[threadid], nextOp->getOutSchema(), attribute);
#endif
	
	return Ready;
}

Operator::ResultCode SortOp::scanStop(unsigned short threadid)
{
	dbgassert(output.at(threadid) != NULL);
	dbgassert(datapage.at(threadid) != NULL);
	dbgassert(state.at(threadid)->remainingdatapos != State::INVALID_OFFSET);

	state[threadid]->remainingdatapos = State::INVALID_OFFSET;

	// Forget data in staging area for thread. 
	// scanStop does not free memory, this is done at threadClose.
	// 
	datapage[threadid]->clear();
	
	return Ready;
}

Operator::GetNextResultT SortOp::getNext(unsigned short threadid)
{
	dbgassert(output.at(threadid) != NULL);
	dbgassert(datapage.at(threadid) != NULL);

	const unsigned int tuplesize = schema.getTupleSize();

	// Create a fake output page to return to user.
	//
	void* tup;
	tup = datapage[threadid]->getTupleOffset(state[threadid]->remainingdatapos);

	unsigned long long remainingtup = gentuples[threadid] - state[threadid]->remainingdatapos;
	unsigned long long canstore = min(static_cast<unsigned long long>(buffsize)/tuplesize, remainingtup);

	Operator::ResultCode rc =
					(canstore == remainingtup) ? Operator::Finished : Operator::Ready;

	state[threadid]->remainingdatapos += canstore;
	return make_pair(rc, new(output[threadid])
									TupleBuffer(tup, canstore * tuplesize, NULL, tuplesize));
}
