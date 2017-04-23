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

#include <algorithm>
#include <cmath>

#include "operators.h"
#include "../util/numaallocate.h"

/**
 * Find position in \a zipfarray array that contains \a tempkey using
 * binary search. Looks only between offsets \a start and \a end.
 */
unsigned long long findidx(const unsigned long long start, 
		const unsigned long long end, double tempkey, double* zipfarray)
{
	if (start >= (end-1))
	{
		if (tempkey < zipfarray[start])
			return start;
		else
			return end;
	}
	else
	{
		const CtLong midpoint = (start + end) / 2;
		if (tempkey < zipfarray[midpoint])
			return findidx(start, midpoint, tempkey, zipfarray);
		else
			return findidx(midpoint, end, tempkey, zipfarray);
	}
}

void ZipfGeneratorOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	IntGeneratorOp::init(root, node);

	assert(node.exists("zipffactor"));

	zipffactor = node["zipffactor"];
}

void ZipfGeneratorOp::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);

	IntGeneratorOp::threadInit(threadid);

	const unsigned long long arrLength = totaltuples/ratio;

	fkarray = (CtLong*) numaallocate_local("ZpfF", arrLength*sizeof(CtLong), this);
	zipfarray = (double*) numaallocate_local("ZpfZ", arrLength*sizeof(double), this);

	for (unsigned long long i = 0; i<arrLength; ++i)
	{
		fkarray[i] = i + 1;
	}	
	std::random_shuffle(fkarray, fkarray+arrLength);

	// Compute PDF: probability of choosing i-th element.
	//
	for (unsigned long long i = 0; i<arrLength; ++i)
	{
		zipfarray[i] = 1.0 / pow((i+1), zipffactor);
	}

	// Compute CDF: cummulative probability of choosing i-th element.
	//
	for (unsigned long long i = 1; i<arrLength; ++i)
	{
		zipfarray[i] = zipfarray[i-1] + zipfarray[i];
	}

	const double sum = zipfarray[arrLength-1];
	for (unsigned long long i = 0; i<arrLength; ++i)
	{
		zipfarray[i] = zipfarray[i]/sum;
	}

	assert(zipfarray[arrLength-1] < 1.0+1e9);
	assert(zipfarray[arrLength-1] > 1.0-1e9);
}

Operator::GetNextResultT ZipfGeneratorOp::getNext(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	GetNextResultT result;
	result = IntGeneratorOp::getNext(threadid);

	// Pass through errors.
	//
	if (result.first == Operator::Error) 
	{
		return result;
	}

	// Update Zipf-distributed FK in place of the original \a ratio value.
	//
	void* tuple;
	unsigned long long tupoffset = 0;
	while ( (tuple = result.second->getTupleOffset(tupoffset++)) ) 
	{
		const unsigned long long arrLength = totaltuples/ratio;

		double tempkey;
		do 
		{
			static_assert(RAND_MAX == 0x7FFFFFFFuLL);
			CtLong randlong = ((CtLong) randintgen.at(threadid)->next() << 31)
				| (randintgen.at(threadid)->next() & 0x7FFFFFFFuLL);
			tempkey = static_cast<double>(randlong) / (1uLL << 62);
		} while (tempkey >= zipfarray[arrLength-1]);

		const unsigned long long idx = findidx(0, arrLength-1, tempkey, zipfarray);

		assert(idx < arrLength);
		assert(tempkey < zipfarray[idx]);
		assert((idx == 0) || (tempkey >= zipfarray[idx - 1]));

		schema.writeData(tuple, 1, &fkarray[idx]);
	}

	return result;
}

void ZipfGeneratorOp::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	IntGeneratorOp::threadClose(threadid);

	dbgassert(zipfarray != NULL);
	dbgassert(fkarray != NULL);

	if (zipfarray) 
	{
		numadeallocate(zipfarray);
	}
	zipfarray = NULL;

	if (fkarray)
	{
		numadeallocate(fkarray);
	}
	fkarray = NULL;
}
