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

#include <mpi.h>

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"

/*
 * In this test, there will be two processes, one running with sender, the other running with
 * receiver. Both process runs with single thread.
 */

const int TUPLES = 20000;

using namespace std;
using namespace libconfig;

Query q1, q2;

ScanOp scannode;
DataSenderBcastMpiOp sendnode;
DataRecverBcastMpiOp receivenode;

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
		of << i  << endl;
	}
	of.close();
}


int main(int argc, char** argv)
{
	int myid, numproc;
	//call mpi initialization
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE,  &provided);
	if (provided < MPI_THREAD_MULTIPLE)
	{
		MPI_Abort(MPI_COMM_WORLD, 0);
	}

	MPI_Comm_size(MPI_COMM_WORLD, &numproc);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);

	MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

	const int buffsize = 1 << 4;

	const char* tmpfileint = "testfileinttoint.tmp";

	Config cfg, cfg2;

	if (myid == 0) {
		createfileint(tmpfileint, TUPLES);
	}

	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// Init scannode
	Setting& scannode1 = cfg.getRoot().add("scan1", Setting::TypeGroup);
	scannode1.add("filetype", Setting::TypeString) = "text";
	scannode1.add("file", Setting::TypeString) = tmpfileint;
	Setting& schemanode1 = scannode1.add("schema", Setting::TypeList);
	schemanode1.add(Setting::TypeString) = "long";

	// Init sendnode
	Setting& sendnode1 = cfg.getRoot().add("sender", Setting::TypeGroup);
	sendnode1.add("nodenum", Setting::TypeInt) = 1;
	sendnode1.add("msgsize", Setting::TypeInt) = 4096;
	sendnode1.add("threadnum", Setting::TypeInt) = 1;
	sendnode1.add("nodeid", Setting::TypeInt) = 0;
	Setting& dest = sendnode1.add("bcastgrp", Setting::TypeList);
	dest.add(Setting::TypeInt) = 0;
	dest.add(Setting::TypeInt) = 1;
	sendnode1.add("tag", Setting::TypeInt) = 1;
	sendnode1.add("myid", Setting::TypeInt) = 0;
	Setting& hash = sendnode1.add("hash", Setting::TypeGroup);
	hash.add("fn", Setting::TypeString) = "modulo";
	hash.add("buckets", Setting::TypeInt) = 1;
	hash.add("field", Setting::TypeInt) = 0;

	cfg2.getRoot().add("path", Setting::TypeString) = "./";
	cfg2.getRoot().add("buffsize", Setting::TypeInt) = buffsize;
	// Init receive node
	Setting& recvnode2 = cfg2.getRoot().add("recver", Setting::TypeGroup);
	recvnode2.add("nodenum", Setting::TypeInt) = 1;
	recvnode2.add("msgsize", Setting::TypeInt) = 4096;
	recvnode2.add("threadnum", Setting::TypeInt) = 1;
	recvnode2.add("nodeid", Setting::TypeInt) = 0;
	recvnode2.add("myid", Setting::TypeInt) = 1;
	Setting& recvtag = recvnode2.add("tag", Setting::TypeList);
	recvtag.add(Setting::TypeInt) = 1;
	Setting& bcastgrp = recvnode2.add("bcastgrp", Setting::TypeList);
	bcastgrp.add(Setting::TypeInt) = 1;
	Setting& bcastsender = recvnode2.add("bcastsender", Setting::TypeList);
	bcastsender.add(Setting::TypeInt) = 0;
	Setting& schemanode2 = recvnode2.add("schema", Setting::TypeList);
	schemanode2.add(Setting::TypeString) = "long";

#ifdef VERBOSE
	cfg.write(stdout);
	cfg2.write(stdout);
#endif

	// build plan tree
	q1.tree = &sendnode;
	sendnode.nextOp = &scannode;

	q2.tree = &receivenode;

	// initialize each node
	if (myid == 0) {
		scannode.init(cfg, scannode1);
		sendnode.init(cfg, sendnode1);
	}
	else if (myid == 1) {
		receivenode.init(cfg2, recvnode2);
	}

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q1.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv2;
	q2.accept(&ppv2);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	MPI_Barrier(MPI_COMM_WORLD);
	if (myid == 0) {
		compute(q1, false);
	}
	else if (myid == 1) {
		compute(q2, true);
	}
	MPI_Barrier(MPI_COMM_WORLD);
	if (myid == 0) {
		q1.destroynofree();
	}
	else if (myid == 1) {
		q2.destroynofree();
	}

	if (myid == 0) {
		deletefile(tmpfileint);
	}

	return 0;
}
