
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
#include "../util/atomics.h"
#include "mpi.h"

#include <fstream>
#include <string>

using std::make_pair;

void DataSenderBcastMpiOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);

	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	threadnum_ = cfg["threadnum"];
	atomic_thread_cnt_ = threadnum_;
	msgsize_ = cfg["msgsize"];
	nodenum_ = cfg["nodenum"];
	assert(nodenum_ < MAX_LINKS);

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1

	int tag;
	// ID of the bcast group, every bcast group includes one sender and all its receiver
	int bcast_grp_id[MAX_LINKS*2];
	tag = cfg["tag"];

	my_mpi_id_ = cfg["myid"];

	libconfig::Setting& bcast_grp = cfg["bcastgrp"];
	int grpsize = bcast_grp.getLength();
	for (int i = 0; i < bcast_grp.getLength(); i++) {
		bcast_grp_id[i] = bcast_grp[i];
	}

	int myid;
	int ret;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	assert(myid == my_mpi_id_);
#ifdef DEBUG2
	cout << "mpi rank " << myid << endl;
#endif

	//build broadcast group
	MPI_Group world_group, broadcast_group;
	MPI_Comm_group(MPI_COMM_WORLD, &world_group);

	MPI_Group_incl(world_group, grpsize, bcast_grp_id, &broadcast_group);
	ret = MPI_Comm_create_group(MPI_COMM_WORLD, broadcast_group, tag, &bcast_comm_);
	assert(ret == MPI_SUCCESS);

	MPI_Group_free(&world_group);
	MPI_Group_free(&broadcast_group);

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++) {
		thread_rc_[i].out_buffer = &EmptyPage;
	}
	// To record how many messages the sender sent out
	msg_num_ = 0;
}


void DataSenderBcastMpiOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	thread_rc_[threadid].buf = numaallocate_local("DSbf", msgsize_, this);
	thread_rc_[threadid].rdma_buf.set((char*)thread_rc_[threadid].buf);
}

Operator::ResultCode DataSenderBcastMpiOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	thread_rc_[threadid].out_buffer = new Page(thread_rc_[threadid].rdma_buf.msg, msgsize_-12, NULL, schema.getTupleSize());
	thread_rc_[threadid].out_buffer->clear();

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderBcastMpiOp::getNext(unsigned short threadid)
{
	Operator::GetNextResultT result;
	int ret;
	result = nextOp->getNext(threadid);
	Page* in;
	Operator::ResultCode rc;
	in = result.second;
	rc = result.first;

	void *tuple;
	int tupoffset = 0;
	while (1) {
		while ((tuple = in->getTupleOffset(tupoffset)) != NULL) {
			//if data in page in is less than left space in send buffer
			uint64_t left_data_in = in->getUsedSpace()-tupoffset*in->getTupleSize();
			if (thread_rc_[threadid].out_buffer->canStore(left_data_in)) {
				void * bucketspace = thread_rc_[threadid].out_buffer->allocate(left_data_in);
				memcpy(bucketspace, tuple, left_data_in);
				break;
			}
			else {
				uint64_t left_space_in_buffer = thread_rc_[threadid].out_buffer->capacity() - thread_rc_[threadid].out_buffer->getUsedSpace();
				void *bucketspace = thread_rc_[threadid].out_buffer->allocate(left_space_in_buffer);
				memcpy(bucketspace, tuple, left_space_in_buffer);
				tupoffset += left_space_in_buffer/in->getTupleSize();
				int datalen = thread_rc_[threadid].out_buffer->getUsedSpace();
				thread_rc_[threadid].rdma_buf.deplete =  MoreData;
				thread_rc_[threadid].rdma_buf.datalen =  datalen;
				thread_rc_[threadid].rdma_buf.nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf.serialize();

				assert(MPI_Ibcast(thread_rc_[threadid].rdma_buf.buffaddr(),
				                  msgsize_, MPI_CHAR, 0, bcast_comm_, &thread_rc_[threadid].request) == MPI_SUCCESS);
				atomic_increment(&msg_num_);

				int flag = 0;
				while (flag != 1) {
					assert(MPI_Test(&thread_rc_[threadid].request, &flag, &thread_rc_[threadid].status) == MPI_SUCCESS);
				}
				thread_rc_[threadid].out_buffer->clear();
			}
		}
		if (rc == Finished) {
			//if this is not the last thread
			if (atomic_increment(&atomic_thread_cnt_, -1) != 1) {
				int datalen = thread_rc_[threadid].out_buffer->getUsedSpace();
				thread_rc_[threadid].rdma_buf.deplete =  MoreData;
				thread_rc_[threadid].rdma_buf.datalen =  datalen;
				thread_rc_[threadid].rdma_buf.nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf.serialize();

				ret = MPI_Ibcast(thread_rc_[threadid].rdma_buf.buffaddr(),
				                 msgsize_, MPI_CHAR, 0, bcast_comm_, &thread_rc_[threadid].request);
				assert(ret == MPI_SUCCESS);
				atomic_increment(&msg_num_);

				int flag = 0;
				while (flag != 1) {
					ret = MPI_Test(&thread_rc_[threadid].request, &flag, &thread_rc_[threadid].status);
					assert(ret == MPI_SUCCESS);
				}
				//clear buffer after send out
				thread_rc_[threadid].out_buffer->clear();
			}
			else { //send out last message
				atomic_increment(&msg_num_);
				int datalen = thread_rc_[threadid].out_buffer->getUsedSpace();
				thread_rc_[threadid].rdma_buf.deplete =  msg_num_;
				thread_rc_[threadid].rdma_buf.datalen =  datalen;
				thread_rc_[threadid].rdma_buf.nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf.serialize();

				ret = MPI_Ibcast(thread_rc_[threadid].rdma_buf.buffaddr(),
				                 msgsize_, MPI_CHAR, 0, bcast_comm_, &thread_rc_[threadid].request);
				assert(ret == MPI_SUCCESS);

				int flag = 0;
				while (flag != 1) {
					ret = MPI_Test(&thread_rc_[threadid].request, &flag, &thread_rc_[threadid].status);
					assert(ret == MPI_SUCCESS);
				}
				//clear buffer after send out
				thread_rc_[threadid].out_buffer->clear();
			}
			return make_pair(Finished, &EmptyPage);
		}

		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}
}

void DataSenderBcastMpiOp::threadClose(unsigned short threadid) {
	numadeallocate(thread_rc_[threadid].buf);
	MPI_Comm_free(&bcast_comm_);
	//delete Page in out_buffer_
	if (thread_rc_[threadid].out_buffer != &EmptyPage) {
		delete thread_rc_[threadid].out_buffer;
	}
}
