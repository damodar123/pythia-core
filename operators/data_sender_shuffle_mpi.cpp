
/*
 * Copyright 2015, Pythia authors (see AUTHORS file).
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

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <iostream>
#include <cstdlib>

#include <iomanip>

#include "../rdtsc.h"
#include "mpi.h"

#include <fstream>
#include <string>

using std::make_pair;

void DataSenderShuffleMpiOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);

	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	hashfn = TupleHasher::create(schema, cfg["hash"]);
	threadnum_ = cfg["threadnum"];
	atomic_thread_cnt_ = threadnum_;
	msgsize_ = cfg["msgsize"];
	nodenum_ = cfg["nodenum"];
	assert(nodenum_ < MAX_LINKS);

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1

	// MPI rank of the process running sender
	my_mpi_id_ = cfg["myid"];

	libconfig::Setting& dest_id_grp = cfg["destid"];
	for (int i = 0; i < dest_id_grp.getLength(); i++) {
		dest_mpi_id_[i] = dest_id_grp[i];
	}
	mpi_tag_ = cfg["mpitag"];

	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	assert(myid == my_mpi_id_);

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].out_buffer[j] = &EmptyPage;
		}
	}
}

void DataSenderShuffleMpiOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	thread_rc_[threadid].buf = numaallocate_local("DSbf", msgsize_*nodenum_, this);
	for (int i=0; i<nodenum_; i++) {
		thread_rc_[threadid].rdma_buf[i].set((char*)thread_rc_[threadid].buf+i*msgsize_);
	}
}

Operator::ResultCode DataSenderShuffleMpiOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	for (int i=0; i<nodenum_; i++) {
		thread_rc_[threadid].out_buffer[i] = new Page(thread_rc_[threadid].rdma_buf[i].msg, msgsize_-12, NULL, schema.getTupleSize());
		thread_rc_[threadid].out_buffer[i]->clear();
	}

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderShuffleMpiOp::getNext(unsigned short threadid)
{
	Operator::GetNextResultT result;
	result = nextOp->getNext(threadid);
	Page* in;
	Operator::ResultCode rc;
	in = result.second;
	rc = result.first;

	void *tuple;
	int tupoffset = 0;
	unsigned int hashbucket;
	while (1) {
		while ((tuple = in->getTupleOffset(tupoffset++)) != NULL) {
			//hash to decide destination
			hashbucket = hashfn.hash(tuple);
			void * bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();

			//now full, already serialized
			if (bucketspace == NULL) {
				int datalen = thread_rc_[threadid].out_buffer[hashbucket]->getUsedSpace();
				thread_rc_[threadid].rdma_buf[hashbucket].deplete =  MoreData;
				thread_rc_[threadid].rdma_buf[hashbucket].datalen =  datalen;
				thread_rc_[threadid].rdma_buf[hashbucket].nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf[hashbucket].serialize();

				assert(MPI_Send(thread_rc_[threadid].rdma_buf[hashbucket].buffaddr(),
				                msgsize_, MPI_CHAR, dest_mpi_id_[hashbucket], mpi_tag_, MPI_COMM_WORLD) == MPI_SUCCESS);
				//clear buffer after send out
				thread_rc_[threadid].out_buffer[hashbucket]->clear();
				bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();
			}
			dbgassert(bucketspace != NULL);
			schema.copyTuple(bucketspace, tuple);
		}
		if (rc == Finished) {
			//if this is not the last thread
			if (atomic_increment(&atomic_thread_cnt_, -1) != 1) {
				for (int i=0; i<nodenum_; i++) {
					int datalen = thread_rc_[threadid].out_buffer[i]->getUsedSpace();
					thread_rc_[threadid].rdma_buf[i].deplete =  MoreData;
					thread_rc_[threadid].rdma_buf[i].datalen =  datalen;
					thread_rc_[threadid].rdma_buf[i].nodeid =  node_id_;
					thread_rc_[threadid].rdma_buf[i].serialize();

					assert(MPI_Send(thread_rc_[threadid].rdma_buf[i].buffaddr(),
					                msgsize_, MPI_CHAR, dest_mpi_id_[i], mpi_tag_, MPI_COMM_WORLD) == MPI_SUCCESS);
					//clear buffer after send out
					thread_rc_[threadid].out_buffer[i]->clear();
				}
			}
			else { //send out last message
				for (int i=0; i<nodenum_; i++) {
					int datalen = thread_rc_[threadid].out_buffer[i]->getUsedSpace();
					thread_rc_[threadid].rdma_buf[i].deplete =  Depleted;
					thread_rc_[threadid].rdma_buf[i].datalen =  datalen;
					thread_rc_[threadid].rdma_buf[i].nodeid =  node_id_;
					thread_rc_[threadid].rdma_buf[i].serialize();

					assert(MPI_Send(thread_rc_[threadid].rdma_buf[i].buffaddr(),
					                msgsize_, MPI_CHAR, dest_mpi_id_[i], mpi_tag_, MPI_COMM_WORLD) == MPI_SUCCESS);
					//clear buffer after send out
					thread_rc_[threadid].out_buffer[i]->clear();
				}
			}
			return make_pair(Finished, &EmptyPage);
		}

		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}
}

void DataSenderShuffleMpiOp::threadClose(unsigned short threadid) {
	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	numadeallocate(thread_rc_[threadid].buf);
	//delete Page in out_buffer_
	for (int i=0; i<MAX_LINKS; i++) {
		if (thread_rc_[threadid].out_buffer[i] != &EmptyPage) {
			delete thread_rc_[threadid].out_buffer[i];
		}
	}
}

