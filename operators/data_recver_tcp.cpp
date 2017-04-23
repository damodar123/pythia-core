
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

#include "../util/tcpsocket.h"

using std::make_pair;

void DataRecverTcpOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	Operator::init(root, cfg);
	schema = Schema::create(cfg["schema"]);
	//todo: the following varaibles should come from config file
	//handle partition attribute and hashing stuff
	nodenum_ = cfg["nodenum"];
	msgsize_ = cfg["msgsize"];
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

	for (int i=0; i<MAX_THREADS; i++) {
		closedsock_cnt_[i] = nodenum_;
	}

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	host_ip_ = (const char*) cfg["hostIP"];
	//vector<char*> dest_ip_
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

	for (int i=0; i<nodenum_; i++) {
		depleted_[i] = 0;
	}
	eos_ = false;
	for (int i=0; i<threadnum_; i++) {
		selectflag_[i] = 0;
		curptr_[i] = 0;
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			validsock_[i][j] = -1;
		}
	}

}

void DataRecverTcpOp::threadInit(unsigned short threadid)
{
	thread_rc_[threadid].buf = numaallocate_local("DRbf", msgsize_, this);
	thread_rc_[threadid].rdma_buf.set(thread_rc_[threadid].buf);

	//buffer of data to up operators
	void *space = numaallocate_local("DRpg", sizeof(Page), this);
	thread_rc_[threadid].output = new (space) Page(buffsize, schema.getTupleSize(), this, "DRpg");

	if (threadid == 0) {
		//build connection here
		//priority is 0>1>2>...>n-1
		//currently, for fragments in one node, they also use tcp and rdma
		//for communication
		//todo optimize for communication in one node
		for (int i=0; i<threadnum_; i++) {
			TcpClient(LISTEN_PORT+i+operator_id_*MAX_THREADS, dest_ip_, nodenum_, sock_id_[i], node_id_);
			maxfd_[i] = -1;
			FD_ZERO(&readfds_[i]);
			for (int j=0; j< nodenum_; j++) {
				if (sock_id_[i][j] > maxfd_[i]) {
					maxfd_[i] = sock_id_[i][j];
				}
				assert(sock_id_[i][j] != -1);
				FD_SET(sock_id_[i][j], &readfds_[i]);
			}

			for (int j=0; j<nodenum_; j++) {
				validsock_[i][j] = sock_id_[i][j];
			}
			//sync to exit
			for (int j=0; j<nodenum_; j++) {
				if(sock_id_[i][j] == -1) {
					continue;
				}
				char temp = 'a';
				assert(send(sock_id_[i][j], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
			}

			for (int j=0; j<nodenum_; j++) {
				if(sock_id_[i][j] == -1) {
					continue;
				}
				char temp = 'b';
				assert(recv(sock_id_[i][j], &temp, sizeof(temp), MSG_WAITALL) != -1);
			}

		}
	}
}

Operator::ResultCode DataRecverTcpOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	thread_rc_[threadid].inter_buff.buff = new Page(schema.getTupleSize(), schema.getTupleSize(), this, "DRib");
	thread_rc_[threadid].inter_buff.clear();
	return Ready;
}

Operator::GetNextResultT DataRecverTcpOp::getNext(unsigned short threadid)
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
				//round up to the multiple size of tuple size
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
		if ((thread_rc_[threadid].inter_buff.deplete == 1) || (eos_ == true)) {
			closedsock_cnt_[threadid]--;

			maxfd_[threadid] = -1;
			validsock_[threadid][thread_rc_[threadid].inter_buff.recv_node] = -1;
			for (int j=0; j< nodenum_; j++) {
				if (validsock_[threadid][j] > maxfd_[threadid]) {
					maxfd_[threadid] = validsock_[threadid][j];
				}
			}

			if (closedsock_cnt_[threadid] == 0) {
				return make_pair(Finished, thread_rc_[threadid].output);
			}
		}

		while ((eos_ == false) && (selectflag_[threadid] == 0)) {
			struct timeval timeout;
			timeout.tv_sec = 2;
			timeout.tv_usec = 1;
			FD_ZERO(&readfds_[threadid]);
			for (int j=0; j< nodenum_; j++) {
				if (validsock_[threadid][j] != -1) {
					FD_SET(sock_id_[threadid][j], &readfds_[threadid]);
				}
			}

			assert((selectflag_[threadid] = select(maxfd_[threadid]+1, &readfds_[threadid], NULL, NULL, &timeout)) >= 0);
			curptr_[threadid] = 0;
		}

		//cout << "recv message" << endl;
		if (eos_ == true) {
			return make_pair(Finished, thread_rc_[threadid].output);
		}

		for (int i=curptr_[threadid]; i<nodenum_; i++) {
			if (FD_ISSET(sock_id_[threadid][i], &readfds_[threadid])) {
				recv(sock_id_[threadid][i], thread_rc_[threadid].rdma_buf.buffaddr(), msgsize_, MSG_WAITALL);

				thread_rc_[threadid].rdma_buf.deserialize();
				int bufind = threadid;
				//build page with data in recv_buf_
				thread_rc_[threadid].inter_buff.buff->resetdata(thread_rc_[threadid].rdma_buf.msg, thread_rc_[threadid].rdma_buf.datalen);
				//get the source id from received data
				thread_rc_[threadid].inter_buff.recv_node = thread_rc_[threadid].rdma_buf.nodeid;
				thread_rc_[threadid].inter_buff.deplete = thread_rc_[threadid].rdma_buf.deplete;
				thread_rc_[threadid].inter_buff.bufind = bufind;
				thread_rc_[threadid].inter_buff.curptr = 0;
				selectflag_[threadid]--;

				curptr_[threadid] = (++i)%nodenum_;
				break;
			}
		}
	}
}

void DataRecverTcpOp::threadClose(unsigned short threadid) {
	//shake hands to exit
	for (int i=0; i<nodenum_; i++) {
		char temp = 'a';
		assert(send(sock_id_[threadid][i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
	}

	for (int i=0; i<nodenum_; i++) {
		char temp = 'b';
		assert(recv(sock_id_[threadid][i], &temp, sizeof(temp), MSG_WAITALL) != -1);
	}

	numadeallocate(thread_rc_[threadid].buf); 	//free buffer
	numadeallocate(thread_rc_[threadid].output);
	for (int i=0; i<nodenum_; i++) {
		close(sock_id_[threadid][i]);
	}
}
