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
#include "libconfig.h++"

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"

/*
 * In this test, there will be two processes, one running with sender, the other running with
 * receiver. Both process runs with single thread. Include tcp/udsr/rcsr/rcread shuffle/bcast.
 */

const int TUPLES = 20000;

using namespace std;
using namespace libconfig;

Query q1s, q1r;

ScanOp scannode;
DataSenderBcastUdSrOp sn_udsr;

DataRecverUdSrOp rn_udsr;

int verify[TUPLES];

void compute(Query q, bool isrecv)
{
	long long tcnt = 1;
	q.threadInit();

	Operator::Page* out;
	Operator::GetNextResultT result;

	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

	while(result.first == Operator::Ready) {
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			if (isrecv) {
				long long v = q.getOutSchema().asLong(tuple, 0);
				if (v <= 0 || v > TUPLES || v != tcnt++)
					fail("Values that never were generated appear in the output stream.");
			}
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

// create file, with content of 1, 2, 3,..., TUPLES
void createfileint(const char* filename, const unsigned int maxnum)
{
	ofstream of(filename);
	for (unsigned int i=1; i<(maxnum+1); ++i)
	{
		of << i << endl;
	}
	of.close();
}


int main()
{
	const int buffsize = 1 << 4;

	const char* tmpfileint = "testfileinttoint.tmp";

	Config cfg;

	createfileint(tmpfileint, TUPLES);

	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// Init scannode
	Setting& scannode1 = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode1.add("filetype", Setting::TypeString) = "text";
	scannode1.add("file", Setting::TypeString) = tmpfileint;
	Setting& schemanode1 = scannode1.add("schema", Setting::TypeList);
	schemanode1.add(Setting::TypeString) = "long";

	// Init sendnode
	Setting& sendnode1 = cfg.getRoot().add("sender", Setting::TypeGroup);
	Setting& port1 = sendnode1.add("port", Setting::TypeList);
	port1.add(Setting::TypeInt) = 12345;
	sendnode1.add("msgsize", Setting::TypeInt) = 4096;
	sendnode1.add("buffnum", Setting::TypeInt) = 8;
	sendnode1.add("qpnum", Setting::TypeInt) = 1;
	sendnode1.add("threadnum", Setting::TypeInt) = 1;
	sendnode1.add("nodenum", Setting::TypeInt) = 1;
	sendnode1.add("nodeid", Setting::TypeInt) = 0;
	sendnode1.add("hostIP", Setting::TypeString) = "127.0.0.1";
	Setting& destIP1 = sendnode1.add("destIP", Setting::TypeList);
	destIP1.add(Setting::TypeString) = "127.0.0.1";
	Setting& hash = sendnode1.add("hash", Setting::TypeGroup);
	hash.add("fn", Setting::TypeString) = "modulo";
	hash.add("buckets", Setting::TypeInt) = 1;
	hash.add("field", Setting::TypeInt) = 0;

	// Init receive node
	Setting& recvnode2 = cfg.getRoot().add("recver", Setting::TypeGroup);
	Setting& port2 = recvnode2.add("port", Setting::TypeList);
	port2.add(Setting::TypeInt) = 12345;
	recvnode2.add("msgsize", Setting::TypeInt) = 4096;
	recvnode2.add("buffnum", Setting::TypeInt) = 8;
	recvnode2.add("qpnum", Setting::TypeInt) = 1;
	recvnode2.add("threadnum", Setting::TypeInt) = 1;
	recvnode2.add("nodenum", Setting::TypeInt) = 1;
	recvnode2.add("nodeid", Setting::TypeInt) = 0;
	recvnode2.add("hostIP", Setting::TypeString) = "127.0.0.1";
	Setting& destIP2 = recvnode2.add("destIP", Setting::TypeList);
	destIP2.add(Setting::TypeString) = "127.0.0.1";
	Setting& schemanode2 = recvnode2.add("schema", Setting::TypeList);
	schemanode2.add(Setting::TypeString) = "long";

#ifdef VERBOSE
	cfg.write(stdout);
#endif

	// build plan tree
	q1s.tree = &sn_udsr;
	sn_udsr.nextOp = &scannode;

	q1r.tree = &rn_udsr;

	// initialize each node
	scannode.init(cfg, scannode1);
	sn_udsr.init(cfg, sendnode1);
	rn_udsr.init(cfg, recvnode2);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q1s.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv2;
	q1r.accept(&ppv2);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	pid_t pid0 = fork();
	if (pid0 == 0) {
		compute(q1s, false);
		exit(0);
	}
	pid_t pid1 = fork();
	if (pid1 == 0) {
		compute(q1r, true);
	}

	int status;
	waitpid(pid0, &status, 0);
	waitpid(pid1, &status, 0);

	q1s.destroynofree();
	q1r.destroynofree();

	deletefile(tmpfileint);

	return 0;
}
