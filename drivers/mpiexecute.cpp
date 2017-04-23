
/*
 * Copyright 2017, Pythia authors (see AUTHORS file).
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
#include <iomanip>
#include <cstdlib>
#include <time.h>
#include "libconfig.h++"

#include "../query.h"
#include "../visitors/allvisitors.h"
#include "../rdtsc.h"
#include "../util/cpufreq.h"

#include <unistd.h>
#include <pwd.h> 
#include <sys/types.h>

#include <mpi.h>
#include <fstream>
#include <sstream>
#include <string>

#define QUERYPLAN

using namespace std;
using namespace libconfig;

const static int MAX_LINKS = 32;

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
	cout << fixed;
	cout << setprecision(2) << "StartTimeInSec: " << timestart.tv_sec + timestart.tv_nsec / 1000000000.0 << endl;
	cout << setprecision(2) << "StopTimeInSec: " << timeend.tv_sec + timeend.tv_nsec / 1000000000.0 << endl;
}

struct mpitop_t 
{
	int rank;
	int nodeid;
} ranktop[MAX_LINKS];

int main(int argc, char** argv)
{
	int myid, numproc;

	// Do MPI initialization
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE,  &provided);
	if (provided < MPI_THREAD_MULTIPLE)
	{
		printf("Error: the MPI library doesn't provide the required MPI_THREAD_MULTIPLE level\n");
		MPI_Abort(MPI_COMM_WORLD, 0);
	}

	MPI_Comm_size(MPI_COMM_WORLD, &numproc);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);

	MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

	// Redirect std output, since we're running two processes in one node.
	struct passwd* pw = getpwuid(getuid());
	const char* pw_name = pw->pw_name; 
	std::ofstream outfile;
	stringstream ss;
	ss << "/tmp/output_" << pw_name << "_" << myid;
	outfile.open(ss.str().c_str(), std::ofstream::trunc);
	std::streambuf *coutbuf = std::cout.rdbuf();
	std::cout.rdbuf(outfile.rdbuf());

	assert(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);

	char* node_id = std::getenv("PBS_NODENUM");
	if (node_id != NULL) 
	{
		cout << "PBS_NODENUM " << node_id << endl;
	}

	cout << "CFLGS: " << MakefileTimestampSignature << endl;

	Config cfg;

	if (argc < 2) {
		cout << "ERROR: Configuration file not specified." << endl;
		cout << "Usage: " << argv[0] << " conf-file" << endl;
		return 2;
	}

	assert(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);

	std::string file_path = argv[1];
	std::stringstream plan_id;
	plan_id << std::setw(2) << std::setfill('0') << myid;
	file_path = file_path + plan_id.str() + ".conf";

	cfg.readFile(file_path.c_str());

	q.create(cfg);

	unsigned long buffsize = cfg.getRoot()["buffsize"];
	cout << "BUFFSIZE: " << buffsize << endl;

	q.threadInit();

	// Barrier before time measurement begins
	assert(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);

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

	assert(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
	q.threadClose();

	q.destroy();

	std::cout.rdbuf(coutbuf);
	
	MPI_Finalize();
	return 0;
}
