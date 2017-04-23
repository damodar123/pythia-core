
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

#include "mpi.h"

using std::make_pair;

void DataRecverMpiOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	// To make sure that thread related structure is cache line aligned
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	Operator::init(root, cfg);
	schema = Schema::create(cfg["schema"]);
	// number of nodes in the cluster
	nodenum_ = cfg["nodenum"];
	// size of one message
	msgsize_ = cfg["msgsize"];
	// number of threads
	threadnum_ = cfg["threadnum"];

	// ID of node
	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	// The MPI rank for the process running this operator, should be unique in the cluster
	my_mpi_id_ = cfg["myid"];
	// tag for this receiver, there may be more than one receiver operator in a process
	// the tag is used to differentiate them.
	mpi_tag_ = cfg["mpitag"];

	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	for (int i=0; i<nodenum_; i++) {
		depleted_.push_back(0);
	}
	eos_ = false;

	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
}

void DataRecverMpiOp::threadInit(unsigned short threadid)
{
	thread_rc_[threadid].buf = numaallocate_local("DRbf", msgsize_, this);
	thread_rc_[threadid].rdma_buf.set(thread_rc_[threadid].buf);

	//buffer of data to up operators
	void *space = numaallocate_local("DRpg", sizeof(Page), this);
	thread_rc_[threadid].output = new (space) Page(buffsize, schema.getTupleSize(), this, "DRpg");
}

Operator::ResultCode DataRecverMpiOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode = Operator::Ready;
	thread_rc_[threadid].inter_buff.buff = new Page(schema.getTupleSize(), schema.getTupleSize(), this, "DRib");
	thread_rc_[threadid].inter_buff.clear();
	return rescode;
}

Operator::GetNextResultT DataRecverMpiOp::getNext(unsigned short threadid)
{
	void *tuple;
	thread_rc_[threadid].output->clear();
	while (1) {
		while ((tuple = (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr))) {
			//output large enough to host all data in thread_rc_[threadid].inter_buff
			size_t leftdata_in_interbuff = thread_rc_[threadid].inter_buff.leftdata();
			if (thread_rc_[threadid].output->canStore(leftdata_in_interbuff)) {
				void *bucketspace = thread_rc_[threadid].output->allocate(leftdata_in_interbuff);
				memcpy(bucketspace, (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr), leftdata_in_interbuff);
				break;
			}
			//output not large enough to host all data in thread_rc_[threadid].inter_buff
			else {
				size_t left_space_in_output = thread_rc_[threadid].output->capacity() - thread_rc_[threadid].output->getUsedSpace();
				size_t remainder = left_space_in_output%thread_rc_[threadid].output->getTupleSize();
				left_space_in_output -= remainder;
				void *bucketspace = thread_rc_[threadid].output->allocate(left_space_in_output);
				memcpy(bucketspace, (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr), left_space_in_output);
				thread_rc_[threadid].inter_buff.curptr += left_space_in_output/(thread_rc_[threadid].inter_buff.buff)->getTupleSize();
				return make_pair(Ready, thread_rc_[threadid].output);
			}
		}

		//if one source notify that it sends all the data, then mark this source as
		//depleted. When all source depleted, return output and Finished
		//even if one source has no data for this dest, it still needs to send a empty
		//buf with deplete set to 1, to indicates the end of stream
		if (thread_rc_[threadid].inter_buff.deplete == 1) {
			depleted_[thread_rc_[threadid].inter_buff.recv_node] = 1;
			for (int i = 0; i<nodenum_; i++) {
				//some channel not eos
				if (depleted_[i] == 0) {
					break;
				}
				//all channel eos
				if (i == (nodenum_-1)) {
					eos_ = true;
					return make_pair(Finished, thread_rc_[threadid].output);
				}
			}
		}

		//poll for one completion
		MPI_Request request;
		MPI_Status status;
		int ret = MPI_Irecv(thread_rc_[threadid].rdma_buf.buffaddr(), msgsize_, MPI_CHAR, MPI_ANY_SOURCE, mpi_tag_, MPI_COMM_WORLD, &request);
		assert(ret == MPI_SUCCESS);
		int flag = 0;
		//poll for completion of mpi recv
		while ((eos_ == false) && (flag != 1)) {
			ret = MPI_Test(&request, &flag, &status);
			assert(ret == MPI_SUCCESS);
		}

		if (eos_ == true) {
			return make_pair(Finished, thread_rc_[threadid].output);
		}

		thread_rc_[threadid].rdma_buf.deserialize();
		int bufind = threadid;
		//build page with data in recv_buf_
		thread_rc_[threadid].inter_buff.buff->resetdata(thread_rc_[threadid].rdma_buf.msg, thread_rc_[threadid].rdma_buf.datalen);
		//get the source id from received data
		thread_rc_[threadid].inter_buff.recv_node = thread_rc_[threadid].rdma_buf.nodeid;
		thread_rc_[threadid].inter_buff.deplete = thread_rc_[threadid].rdma_buf.deplete;
		thread_rc_[threadid].inter_buff.bufind = bufind;
		thread_rc_[threadid].inter_buff.curptr = 0;
	}
}

void DataRecverMpiOp::threadClose(unsigned short threadid) {
	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	numadeallocate(thread_rc_[threadid].buf);
	//free buffer
	numadeallocate(thread_rc_[threadid].output);
}
