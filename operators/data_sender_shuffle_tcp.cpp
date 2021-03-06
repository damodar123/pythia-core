
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
#include "../util/tcpsocket.h"

#include <fstream>
#include <string>

using std::make_pair;

//input attributes for rdma send:
// int port: port used for TCP/IP to do sync
// int msgsize: send message size, 4096 bytes by default
// int buffnum: number of buffers used in send

void DataSenderShuffleTcpOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	//static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	//static_assert(sizeof(struct destlink_t)%CACHE_LINE_SIZE == 0);

	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	//todo: the following varaibles should come from config file
	//handle partition attribute and hashing stuff
	hashfn = TupleHasher::create(schema, cfg["hash"]);
	if (cfg.exists("threadnum")) {
		threadnum_ = cfg["threadnum"];
	}
	else {
		threadnum_ = 1;
	}
	if (cfg.exists("opid")) {
		operator_id_ = cfg["opid"];
	}
	else {
		operator_id_ = 0;
	}

	atomic_thread_cnt_ = threadnum_;
	msgsize_ = cfg["msgsize"];
	nodenum_ = cfg["nodenum"];
	assert(nodenum_ < MAX_LINKS);

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	host_ip_ = (const char*) cfg["hostIP"];
	//loop to get all ip adress of remote node
	libconfig::Setting& ipgrp = cfg["destIP"];
	int size = ipgrp.getLength();

	assert( size != 0 );
	assert( size <= MAX_LINKS );

	for (int i=0; i<size; ++i) {
		std::string ipaddr = (const char*) ipgrp[i];
		dest_ip_.push_back(ipaddr);
	}
	vector<int> temp_sock_id;
	for (int i=0; i<size; i++) {
		temp_sock_id.push_back(-1);
	}
	for (int i=0; i<threadnum_; i++) {
		sock_id_.push_back(temp_sock_id);
	}

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].out_buffer[j] = &EmptyPage;
		}
	}
}

void DataSenderShuffleTcpOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	thread_rc_[threadid].buf = numaallocate_local("DSbf", msgsize_*nodenum_, this);
	for (int i=0; i<nodenum_; i++) {
		thread_rc_[threadid].rdma_buf[i].set((char*)thread_rc_[threadid].buf+i*msgsize_);
	}

	if (threadid == 0) {
		for (int i=0; i<threadnum_; i++) {
			TcpServer(LISTEN_PORT+i+operator_id_*MAX_THREADS, host_ip_.c_str(), nodenum_, sock_id_[i]);
			for (int j=0; j<nodenum_; j++) {
				assert(sock_id_[i][j] != -1);
			}
			//sync to exit
			for (int j=0; j<nodenum_; j++) {
				if(sock_id_[i][j] == -1) {
					continue;
				}
				char temp = 'a';
				assert(send(sock_id_[i][j], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
				//cout << "send hand sig from " << node_id_ << " to " << j << endl;
			}

			for (int j=0; j<nodenum_; j++) {
				if(sock_id_[i][j] == -1) {
					continue;
				}
				char temp = 'b';
				assert(recv(sock_id_[i][j], &temp, sizeof(temp), MSG_WAITALL) != -1);
				//cout << "recv hand sig " << temp << " from " << j << " to " << node_id_ << endl;
			}

		}
	}
}

Operator::ResultCode DataSenderShuffleTcpOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	//cout << "before scan start" << endl;
	ResultCode rescode;

	for (int i=0; i<nodenum_; i++) {
		thread_rc_[threadid].out_buffer[i] = new Page(thread_rc_[threadid].rdma_buf[i].msg, msgsize_-12, NULL, schema.getTupleSize());
		thread_rc_[threadid].out_buffer[i]->clear();
	}

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderShuffleTcpOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
	//cout << "before getnext" << endl;
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

			if (bucketspace == NULL) {
				int datalen = thread_rc_[threadid].out_buffer[hashbucket]->getUsedSpace();
				thread_rc_[threadid].rdma_buf[hashbucket].deplete =  MoreData;
				thread_rc_[threadid].rdma_buf[hashbucket].datalen =  datalen;
				thread_rc_[threadid].rdma_buf[hashbucket].nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf[hashbucket].serialize();

				send(sock_id_[threadid][hashbucket], thread_rc_[threadid].rdma_buf[hashbucket].buffaddr(), msgsize_, MSG_WAITALL);
				//clear buffer after send out
				thread_rc_[threadid].out_buffer[hashbucket]->clear();
				bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();
			}
			dbgassert(bucketspace != NULL);
			schema.copyTuple(bucketspace, tuple);
		}
		if (rc == Finished) {
			for (int i=0; i<nodenum_; i++) {
				int datalen = thread_rc_[threadid].out_buffer[i]->getUsedSpace();
				thread_rc_[threadid].rdma_buf[i].deplete =  Depleted;
				thread_rc_[threadid].rdma_buf[i].datalen =  datalen;
				thread_rc_[threadid].rdma_buf[i].nodeid =  node_id_;
				thread_rc_[threadid].rdma_buf[i].serialize();

				send(sock_id_[threadid][i], thread_rc_[threadid].rdma_buf[i].buffaddr(), msgsize_, MSG_WAITALL);
				//clear buffer after send out
				thread_rc_[threadid].out_buffer[i]->clear();
			}
			return make_pair(Finished, &EmptyPage);
		}

		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}
}

void DataSenderShuffleTcpOp::threadClose(unsigned short threadid) {
	//shake hands to exit
	//block until all destinations receive all the data
	//is this necessary? or should we put this in scan stop?
	//cout << "before t close" << endl;
	for (int i=0; i<nodenum_; i++) {
		char temp = 'a';
		assert(send(sock_id_[threadid][i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
		//cout << "send hand sig from " << node_id_ << " to " << i << endl;
	}

	for (int i=0; i<nodenum_; i++) {
		char temp = 'b';
		assert(recv(sock_id_[threadid][i], &temp, sizeof(temp), MSG_WAITALL) != -1);
		//cout << "recv hand sig " << temp << " from " << i << " to " << node_id_ << endl;
	}

	numadeallocate(thread_rc_[threadid].buf);
	//delete Page in out_buffer_
	for (int i=0; i<MAX_LINKS; i++) {
		if (thread_rc_[threadid].out_buffer[i] != &EmptyPage) {
			delete thread_rc_[threadid].out_buffer[i];
		}
	}

	for (int i=0; i<nodenum_; i++) {
		close(sock_id_[threadid][i]);
	}
}
