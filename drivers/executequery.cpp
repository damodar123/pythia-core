
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
#include <cstdlib>
#include <ctime>
#include "libconfig.h++"

#include "../query.h"
#include "../visitors/allvisitors.h"
#include "../rdtsc.h"
#include "../util/cpufreq.h"

#define QUERYPLAN

using namespace std;
using namespace libconfig;

void fail(const char* explanation) {
	std::cout << " ** FAILED: " << explanation << std::endl;
	throw QueryExecutionError();
}

extern size_t TotalBytesAllocated;
extern const char* MakefileTimestampSignature;

#ifdef STATS_ALLOCATE
void dbgPrintAllocations(Query& q);
void dbgPrintTimeAlloc();
void dbgStartTimeAlloc();
void dbgStopTimeAlloc();
#endif

Query q;

void compute() 
{
	timespec timestart, timeend;
	unsigned long long cycles = 0;
	Operator::GetNextResultT result; 
	result.first = Operator::Ready;
	
	startTimer(&cycles);
	clock_gettime(CLOCK_MONOTONIC, &timestart);

	if (q.scanStart() == Operator::Error)
		fail("Scan initialization failed.");

	while(result.first == Operator::Ready) {
		result = q.getNext();

		if (result.first == Operator::Error)
			fail("GetNext returned error.");

		Operator::Page::Iterator it = result.second->createIterator();
		void* tuple;
		while ((tuple = it.next()) ) {
			cout << q.getOutSchema().prettyprint(tuple, '|') << endl;
		}
	}

	if (q.scanStop() == Operator::Error) 
		fail("Scan stop failed.");

	stopTimer(&cycles);
	clock_gettime(CLOCK_MONOTONIC, &timeend);

	cout << "ResponseTimeInCycles: " << cycles << endl; 
	cout << "ResponseTimeInSec: " 
		<< (timeend.tv_sec-timestart.tv_sec) + (timeend.tv_nsec - timestart.tv_nsec)/1000000000.0 << endl;
}


int main(int argc, char** argv)
{
	cout << "CFLGS: " << MakefileTimestampSignature << endl;

	Config cfg;

	if (argc < 2) {
		cout << "ERROR: Configuration file not specified." << endl;
		cout << "Usage: " << argv[0] << " conf-file" << endl;
		return 2;
	}

	cfg.readFile(argv[1]);
	q.create(cfg);

	unsigned long buffsize = cfg.getRoot()["buffsize"];
	cout << "BUFFSIZE: " << buffsize << endl;

	q.threadInit();

#ifdef STATS_ALLOCATE
	dbgStartTimeAlloc();
#endif

	compute();

#ifdef STATS_ALLOCATE
	dbgStopTimeAlloc();
#endif

#ifdef QUERYPLAN
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	cout << "Total Memory Allocated (bytes): " << TotalBytesAllocated << endl;

#ifdef STATS_ALLOCATE
	dbgPrintAllocations(q);
	dbgPrintTimeAlloc();
#endif

	q.threadClose();

	q.destroy();

	return 0;
}
