
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

#include "../util/atomics.h"
#include "mpi.h"

#include <fstream>
#include <string>

using std::make_pair;

void DataRecverBcastMpiOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	// Make sure that the thread_rc_t struct is cache aligned to avoid false sharing
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	Operator::init(root, cfg);
	schema = Schema::create(cfg["schema"]);
	// The number of nodes
	nodenum_ = cfg["nodenum"];
	// Size of one message
	msgsize_ = cfg["msgsize"];
	// number of threads involved in communication
	threadnum_ = cfg["threadnum"];

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	// depleted_[i] is set to 1 if received the the last message from node i
	for (int i=0; i<nodenum_; i++) {
		depleted_.push_back(0);
	}
	// set to true after received all messages
	eos_ = false;
	// mpi rank of the process running this operator
	my_mpi_id_ = cfg["myid"];

	// mpi code which will be used to build the mpi broadcast group
	int tag[MAX_LINKS];
	libconfig::Setting& tag_cfg = cfg["tag"];
	for (int i = 0; i < tag_cfg.getLength(); i++) {
		tag[i] = tag_cfg[i];
	}

	int bcast_grp_id[MAX_LINKS*2];
	libconfig::Setting& bcast_grp = cfg["bcastgrp"];
	int grpsize = bcast_grp.getLength();
	for (int i = 0; i < bcast_grp.getLength(); i++) {
		bcast_grp_id[i+1] = bcast_grp[i];
	}

	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	assert(myid == my_mpi_id_);
#ifdef DEBUG2
	cout << "mpi rank " << myid << endl;
#endif

	//build broadcast group
	MPI_Group world_group, broadcast_group[MAX_LINKS];
	MPI_Comm_group(MPI_COMM_WORLD, &world_group);
	int ret;

	libconfig::Setting& bcast_sender = cfg["bcastsender"];

	for (int i=0; i<nodenum_; i++) {
		bcast_grp_id[0] = bcast_sender[i];
#ifdef DEBUG2
		for (int k=0; k<=grpsize; k++) {
			std::cout << "group " << i << " bcast " << bcast_grp_id[k] << std::endl;
		}
#endif
		MPI_Group_incl(world_group, (grpsize+1), bcast_grp_id, &broadcast_group[i]);
		ret = MPI_Comm_create_group(MPI_COMM_WORLD, broadcast_group[i], tag[i], &bcast_comm_[i]);
		assert(ret == MPI_SUCCESS);
	}

	MPI_Group_free(&world_group);
	for (int i=0; i<nodenum_; i++) {
		MPI_Group_free(&broadcast_group[i]);
	}

	for (int i = 0; i < MAX_LINKS; ++i) {
		// messages actually received
		msg_num_[i] = 0;
		// record the expected number of messages to be received, exit only when msg_num_ and
		// expected_ match
		expected_[i] = 0;
	}
}

void DataRecverBcastMpiOp::threadInit(unsigned short threadid)
{
	thread_rc_[threadid].buf = numaallocate_local("DRbf", msgsize_*nodenum_, this);
	for (int i=0; i<nodenum_; i++) {
		thread_rc_[threadid].rdma_buf[i].set((char*)thread_rc_[threadid].buf+i*msgsize_);
	}

	//buffer of data to up operators
	void *space = numaallocate_local("DRpg", sizeof(Page), this);
	thread_rc_[threadid].output = new (space) Page(buffsize, schema.getTupleSize(), this, "DRpg");
}

Operator::ResultCode DataRecverBcastMpiOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode = Operator::Ready;
	int ret = 0;
	thread_rc_[threadid].inter_buff.buff = new Page(schema.getTupleSize(), schema.getTupleSize(), this, "DRib");
	thread_rc_[threadid].inter_buff.clear();

	for (int i=0; i<nodenum_; i++) {
		ret = MPI_Ibcast(thread_rc_[threadid].rdma_buf[i].buffaddr(), msgsize_, MPI_CHAR,
		                  0, bcast_comm_[i], &thread_rc_[threadid].request[i]);
		assert(ret == MPI_SUCCESS);
	}
	return rescode;
}

Operator::GetNextResultT DataRecverBcastMpiOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
	//poll for recv completion
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
		if (thread_rc_[threadid].inter_buff.deplete != 0) {
			expected_[thread_rc_[threadid].inter_buff.recv_node] = thread_rc_[threadid].inter_buff.deplete;
		}
		if ((expected_[thread_rc_[threadid].inter_buff.recv_node] != 0) &&
		        (expected_[thread_rc_[threadid].inter_buff.recv_node]
		         <= msg_num_[thread_rc_[threadid].inter_buff.recv_node])) {
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
		//
		if (thread_rc_[threadid].inter_buff.recv_node != -1) {
			int recvnode = thread_rc_[threadid].inter_buff.recv_node;
			assert(MPI_Ibcast(thread_rc_[threadid].rdma_buf[recvnode].buffaddr(), msgsize_, MPI_CHAR, 0,
			                  bcast_comm_[recvnode], &thread_rc_[threadid].request[recvnode]) == MPI_SUCCESS);
		}
		int flag = 0, srcid = -1;
		while ((flag == 0) && (eos_ == false)) {
			for (int i=0; i<nodenum_; i++) {
				assert(MPI_Test(&thread_rc_[threadid].request[i], &flag, &thread_rc_[threadid].status[i]) == MPI_SUCCESS);
				if (flag == 1) {
					srcid = i;
					atomic_increment(&msg_num_[srcid]);
					break;
				}
			}
		}

		if (eos_ == true) {
			return make_pair(Finished, thread_rc_[threadid].output);
		}

		thread_rc_[threadid].rdma_buf[srcid].deserialize();
		int bufind = threadid;
		//build page with data in recv_buf_
		thread_rc_[threadid].inter_buff.buff->resetdata(thread_rc_[threadid].rdma_buf[srcid].msg, thread_rc_[threadid].rdma_buf[srcid].datalen);
		//get the source id from received data
		thread_rc_[threadid].inter_buff.recv_node = thread_rc_[threadid].rdma_buf[srcid].nodeid;
		thread_rc_[threadid].inter_buff.deplete = thread_rc_[threadid].rdma_buf[srcid].deplete;
		thread_rc_[threadid].inter_buff.bufind = bufind;
		thread_rc_[threadid].inter_buff.curptr = 0;
	}
}

void DataRecverBcastMpiOp::threadClose(unsigned short threadid) {
	int myid;
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);

	for (int i=0; i<nodenum_; i++) {
		MPI_Comm_free(&bcast_comm_[i]);
	}
	numadeallocate(thread_rc_[threadid].buf);
	//free buffer
	numadeallocate(thread_rc_[threadid].output);
}
