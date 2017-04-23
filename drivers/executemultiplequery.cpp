
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
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <ctime>
#include <sys/types.h>
#include <sys/wait.h>
#include "libconfig.h++"

#include "../query.h"
#include "../visitors/allvisitors.h"
#include "../rdtsc.h"
#include "../util/cpufreq.h"

#include <unistd.h>
#include <pwd.h>

//header for tcp/ip connection
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <arpa/inet.h>

#include "../util/tcpsocket.h"

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

Query q[32];

const static int MAX_LINKS = 32;
const static int SYNC_PORT = 19891;
vector<int> sock_id (128, -1);
pid_t pid[32];
string coord_ip;
vector<int> globalFragId;
int clientnum;

void compute(int index, std::ofstream& outfile) 
{
	timespec timestart, timeend;
	unsigned long long cycles = 0;
	Operator::GetNextResultT result; 
	result.first = Operator::Ready;
	
	startTimer(&cycles);
	clock_gettime(CLOCK_MONOTONIC, &timestart);

	if (q[index].scanStart() == Operator::Error)
		fail("Scan initialization failed.");

	while(result.first == Operator::Ready) {
		result = q[index].getNext();

		if (result.first == Operator::Error)
			fail("GetNext returned error.");

		Operator::Page::Iterator it = result.second->createIterator();
		void* tuple;
		while ((tuple = it.next()) ) {
			outfile << q[index].getOutSchema().prettyprint(tuple, '|') << endl;
		}
	}

	if (q[index].scanStop() == Operator::Error) 
		fail("Scan stop failed.");

	stopTimer(&cycles);
	clock_gettime(CLOCK_MONOTONIC, &timeend);

	outfile << "ResponseTimeInCycles: " << cycles << endl; 
	outfile << "ResponseTimeInSec: " 
		<< (timeend.tv_sec-timestart.tv_sec) + (timeend.tv_nsec - timestart.tv_nsec)/1000000000.0 << endl;
	outfile << fixed;
	outfile << setprecision(2) << "StartTimeInSec: " << timestart.tv_sec + timestart.tv_nsec / 1000000000.0 << endl;
	outfile << setprecision(2) << "StopTimeInSec: " << timeend.tv_sec + timeend.tv_nsec / 1000000000.0 << endl;
}

int runquery(int index);
int main(int argc, char** argv)
{
	cout << "CFLGS: " << MakefileTimestampSignature << endl;

	Config cfg;

	if (argc < 2) {
		cout << "ERROR: Configuration file not specified." << endl;
		cout << "Usage: " << argv[0] << " conf-file" << endl;
		return 2;
	}

	assert(argc <= 33);
	for (int i=1; i<argc; i++) {
		cfg.readFile(argv[i]);
		q[i-1].create(cfg);

		//only node nodeid 0 and final agg fragment can act as coordinate	
		int nodeid = cfg.getRoot()["nodeid"];
		int fragnum = cfg.getRoot()["fragnum"];
		int fragid = cfg.getRoot()["fragid"];
		int nodenum = cfg.getRoot()["nodenum"];
		clientnum = (fragnum - 1)*nodenum;
		globalFragId.push_back((fragnum-1)*nodeid+fragid);
		if (i == 1) {
			string temp_coord_ip = cfg.getRoot()["hostIP"];
			coord_ip = temp_coord_ip;
		}
	}

	unsigned long buffsize = cfg.getRoot()["buffsize"];
	cout << "BUFFSIZE: " << buffsize << endl;
 
	for (int i=0; i<argc-1; i++) {
		pid[i] = fork();
		if (pid[i] == 0) {
			runquery(i);
			exit(0);
		}
		printf("%d, %d\n", i, pid[i]);
	}
	int status;
	for (int i=0; i<argc-1; i++) {
		waitpid(pid[i], &status, 0);
	}
	return 0;
}

int runquery(int index) {
	//get current user name
	struct passwd* pw = getpwuid(getuid());
	const char* pw_name = pw->pw_name;

	std::ofstream outfile;
	stringstream ss;
	ss << "/tmp/output_" << pw_name << "_" << index;
	outfile.open(ss.str().c_str());
	std::streambuf *coutbuf = std::cout.rdbuf();
	std::cout.rdbuf(outfile.rdbuf());

	if (globalFragId[index] == 0) {
		TcpServer(SYNC_PORT, coord_ip.c_str(), clientnum, sock_id);
		for (int i=0; i<clientnum; i++) {
			assert(sock_id[i] != -1);
		}
	}
	else {
		vector<string> temp;
		temp.push_back(coord_ip);
		TcpClient(SYNC_PORT, temp, 1, sock_id, globalFragId[index]-1);
		assert(sock_id[0] != -1);
	}

	//sync before execute
	if (globalFragId[index] == 0) {
		char temp;
		for (int i=0; i<clientnum; i++) {
			assert(recv(sock_id[i], &temp, sizeof(temp), MSG_WAITALL) == 1);
		}

		for (int i=0; i<clientnum; i++) {
			assert(send(sock_id[i], &temp, sizeof(temp), MSG_DONTWAIT) == 1);
		}
	}
	else {
		char temp = 'r';
		assert(send(sock_id[0], &temp, sizeof(temp), MSG_DONTWAIT) == 1);
		assert(recv(sock_id[0], &temp, sizeof(temp), MSG_WAITALL) == 1);
	}

	q[index].threadInit();

	//sync before execute
	if (globalFragId[index] == 0) {
		char temp;
		for (int i=0; i<clientnum; i++) {
			assert(recv(sock_id[i], &temp, sizeof(temp), MSG_WAITALL) == 1);
		}

		for (int i=0; i<clientnum; i++) {
			assert(send(sock_id[i], &temp, sizeof(temp), MSG_DONTWAIT) == 1);
		}
	}
	else {
		char temp = 'r';
		assert(send(sock_id[0], &temp, sizeof(temp), MSG_DONTWAIT) == 1);
		assert(recv(sock_id[0], &temp, sizeof(temp), MSG_WAITALL) == 1);
	}

#ifdef STATS_ALLOCATE
	dbgStartTimeAlloc();
#endif

	compute(index, outfile);

#ifdef STATS_ALLOCATE
	dbgStopTimeAlloc();
#endif

#ifdef QUERYPLAN
	outfile << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q[index].accept(&ppv);
	outfile << "----------- QUERY PLAN END -----------" << endl;
#endif

	outfile << "Total Memory Allocated (bytes): " << TotalBytesAllocated << endl;

#ifdef STATS_ALLOCATE
	dbgPrintAllocations(q[index]);
	dbgPrintTimeAlloc();
#endif

	q[index].threadClose();

	if (globalFragId[index] == 0) {
		for (int i=0; i<clientnum; i++) {
			close(sock_id[i]);
		}
	}
	else {
		close(sock_id[0]);
	}

	q[index].destroy();

	std::cout.rdbuf(coutbuf);
	outfile.close();
	return 0;
}
