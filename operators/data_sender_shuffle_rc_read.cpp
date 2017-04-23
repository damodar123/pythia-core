
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

// Enable define to turn on the tracing facility.
//
// The tracing facility records events at the global static TraceLog array.
// This simplifies allocation, reclamation and object-oriented clutter, at the
// cost of producing meaningless output if more than one HashJoins are being
// executed in parallel.
//
// #define TRACESENDLOG

#ifdef TRACESENDLOG

#include "../rdtsc.h"
union TraceEntry
{
	struct
	{
		unsigned char garbage[7];
		unsigned char label;
	};
	unsigned long long tick;
};

static const unsigned long TraceLogSize = 0x100000;
static const unsigned long TraceMaxThreads = 32;
static TraceEntry TraceLog[TraceLogSize*TraceMaxThreads];
static unsigned long TraceLogTail[TraceMaxThreads] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  << 16 zeros
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

static void append_to_log(unsigned char label, unsigned short threadid)
{
	static_assert(sizeof(unsigned long long) == 8);

	assert(TraceLogTail[threadid] < TraceLogSize);
	unsigned long e = TraceLogTail[threadid] + TraceLogSize * threadid;
	TraceLog[e].tick = curtick();
	TraceLog[e].label = label;

	++TraceLogTail[threadid];

}

#include<fstream>
#include<string.h>
#include<time.h>

static void dbgDumpTraceToFile(const char* fileprefix)
{
	char filename[1024];
	filename[1023]=0;

	time_t curtime = time(0);
	strftime(filename, 1023, "%F_%T_", localtime(&curtime));
	strncpy(filename, fileprefix, 1023);
	ofstream of(filename);
	for (unsigned long threadid=0; threadid<TraceMaxThreads; ++threadid)
	{
		for (unsigned long i=0; i<TraceLogTail[threadid]; ++i)
		{
			of << threadid << " ";
			of << TraceLog[i + TraceLogSize * threadid].label << " ";
			of << (TraceLog[i + TraceLogSize * threadid].tick
			       & (0x00FFFFFFFFFFFFFFuLL)) << endl;
		}
	}
	of.close();
}

#define TRACE( x ) append_to_log( x , threadid )
#define DUMP( filename ) dbgDumpTraceToFile( filename );
#else
#define TRACE( x ) ;
#define DUMP( filename ) ;
#endif

//to build Rdma rc connection for data transmit
int DataSenderShuffleRcReadOp::RdmaDataChannelConnect(int qpid) {
	int ret = -1;

	int numdev;
	struct ibv_device **ibdevlist = ibv_get_device_list(&numdev);

	if (ibname_.empty() == false)
	{
		for (int i=0; i<numdev; i++)
		{
			if (strcmp(ibv_get_device_name(ibdevlist[i]), ibname_.c_str()) == 0)
			{
				qp_rc_[qpid].ctx = ibv_open_device(ibdevlist[i]);
				break;
			}
		}
	}
	else
	{
		qp_rc_[qpid].ctx = ibv_open_device(ibdevlist[0]);
	}
	assert(qp_rc_[qpid].ctx != NULL);

	ibv_free_device_list(ibdevlist);
	qp_rc_[qpid].pd  = ibv_alloc_pd(qp_rc_[qpid].ctx);
	assert(qp_rc_[qpid].pd != NULL);
	qp_rc_[qpid].cq  = ibv_create_cq(qp_rc_[qpid].ctx, buffnum_/qpnum_*4+1, NULL, NULL, 0);
	assert(qp_rc_[qpid].cq != NULL);
	for (unsigned int i = 0; i<sock_id_.size(); i++) {
		struct ibv_qp_init_attr qp_init_attr;
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.send_cq = qp_rc_[qpid].cq;
		qp_init_attr.recv_cq = qp_rc_[qpid].cq;
		//set to buffnum_ when used for data transfer, otherwise 1
		qp_init_attr.cap.max_send_wr  = buffnum_/qpnum_*2;
		qp_init_attr.cap.max_recv_wr  = buffnum_/qpnum_*2;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 0;
		qp_init_attr.qp_type = IBV_QPT_RC;

		qp_rc_[qpid].qp[i] = ibv_create_qp(qp_rc_[qpid].pd, &qp_init_attr);
		assert(qp_rc_[qpid].qp[i] != NULL);
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state   = IBV_QPS_INIT;
		qp_attr.pkey_index   = 0;
		qp_attr.port_num   = 1;
		qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

		ret = ibv_modify_qp(qp_rc_[qpid].qp[i], &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
		assert(ret == 0);
		struct ibv_port_attr ptattr;
		ibv_query_port(qp_rc_[qpid].ctx, 1, &ptattr);

		local_data_qp_info_[i].lid = ptattr.lid;
		local_data_qp_info_[i].qpn = qp_rc_[qpid].qp[i]->qp_num;
		local_data_qp_info_[i].psn = rand() & 0xffffff;
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			assert(send(sock_id_[i], &local_data_qp_info_[i], sizeof(local_data_qp_info_[i]), MSG_DONTWAIT) != -1);
		}
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			assert(recv(sock_id_[i], &remote_data_qp_info_[i], sizeof(remote_data_qp_info_[i]), MSG_WAITALL) != -1);

			struct ibv_qp_attr conn_attr;
			memset(&conn_attr, 0, sizeof(conn_attr));
			conn_attr.qp_state     = IBV_QPS_RTR;
			conn_attr.path_mtu     = IBV_MTU_4096;
			conn_attr.dest_qp_num    = remote_data_qp_info_[i].qpn;
			conn_attr.rq_psn       = remote_data_qp_info_[i].psn;
			conn_attr.ah_attr.is_global      = 0;
			conn_attr.ah_attr.dlid       = remote_data_qp_info_[i].lid;
			conn_attr.ah_attr.sl         = 0;
			conn_attr.ah_attr.src_path_bits    = 0;
			conn_attr.ah_attr.port_num     = 1;
			conn_attr.max_dest_rd_atomic     = 1;

			int rtr_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN
			                | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
			ret = ibv_modify_qp(qp_rc_[qpid].qp[i], &conn_attr, rtr_flags);
			assert(ret == 0);

			memset(&conn_attr, 0, sizeof(conn_attr));
			conn_attr.qp_state      = IBV_QPS_RTS;
			conn_attr.sq_psn      = local_data_qp_info_[i].psn;
			conn_attr.timeout = 14; //consider to increase this when scale is large
			conn_attr.retry_cnt = 12;
			conn_attr.rnr_retry = 7;
			conn_attr.path_mig_state = IBV_MIG_REARM;
			conn_attr.max_rd_atomic = 1;

			int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT
			                | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

			ret = ibv_modify_qp(qp_rc_[qpid].qp[i], &conn_attr, rts_flags);
			assert(ret == 0);
		}
	}
	return 0;
}

//input attributes for rdma send:
// int port: port used for TCP/IP to do sync
// int msgsize: send message size, 4096 bytes by default
// int buffnum: number of buffers used in send

void DataSenderShuffleRcReadOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	hashfn = TupleHasher::create(schema, cfg["hash"]);
	buffnum_ = cfg["buffnum"];
	nodenum_ = cfg["nodenum"];
	if (cfg.exists("threadnum")) {
		threadnum_ = cfg["threadnum"];
	}
	else {
		threadnum_ = 1;
	}
	msgsize_ = cfg["msgsize"];
	if (cfg.exists("qpnum")) {
		qpnum_ = cfg["qpnum"];
	}
	else {
		qpnum_ = 1;
	}

	buffperthd_ = buffnum_/threadnum_;

	//need to ensure that every thread has one buffer for each dest
	assert(buffperthd_ >= nodenum_);
	assert(buffperthd_ <= MAX_QP_DEPTH);

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	host_ip_ = (const char*) cfg["hostIP"];
	//loop to get all ip adress of remote node
	libconfig::Setting& ipgrp = cfg["destIP"];
	int size = ipgrp.getLength();

	if (cfg.exists("opid"))
	{
		operator_id_ = cfg["opid"];
	}
	else
	{
		operator_id_ = 0;
	}

	if (cfg.exists("ibname"))
	{
		ibname_ = (const char*)cfg["ibname"];
	}

	assert( size != 0 );
	assert( size <= MAX_LINKS );

	for (int i=0; i<size; ++i) {
		std::string ipaddr = (const char*) ipgrp[i];
		dest_ip_.push_back(ipaddr);
	}
	//vector<int> sock_id_
	for (int i=0; i<size; i++) {
		sock_id_.push_back(-1);
	}

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].out_buffer[j] = &EmptyPage;
			thread_rc_[i].out_buffer_id[j] = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			for (int k=0; k<MAX_BUFNUM; k++) {
				thread_rc_[i].free_buf_id[j][k] = -1;
			}
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<buffperthd_; j++) {
			thread_rc_[i].free_buf_id[0][j] = j;
		}
	}
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].localptr[j] = 0;
		}
	}
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].remoteptr[j] = 0;
		}
	}

	for (int i=0; i<threadnum_; i++) {
		tid2qpid_[i] = i*qpnum_/threadnum_;
	}
	pthread_barrier_init(&barrier_, NULL, threadnum_);
}

void DataSenderShuffleRcReadOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	//create buffers for rdma
	rdma_buf_[threadid] = numaallocate_local("DSbf", (size_t)msgsize_*buffperthd_, this);
	memset(rdma_buf_[threadid], 0, (size_t)msgsize_*buffperthd_);

	//create memory space for the struct
	send_buf_[threadid] = (struct RdmaBuf *)numaallocate_local("DSst", sizeof(struct RdmaBuf)*buffperthd_, this);
	memset(send_buf_[threadid], 0, sizeof(struct RdmaBuf)*buffperthd_);

	//associate each RdmaBuf with its corresponding buffer
	for (int i=0; i<buffperthd_; i++) {
		send_buf_[threadid][i].set((char*)rdma_buf_[threadid]+i*msgsize_);
	}


	if (threadid == 0) {
		//build connection here
		TcpServer(LISTEN_PORT+operator_id_, host_ip_.c_str(), nodenum_, sock_id_);
		for (unsigned int i=0; i<port_.size(); i++) {
			assert(sock_id_[i] != -1);
		}

		for (int i=0; i<qpnum_; i++) {
			RdmaDataChannelConnect(i);
		}
	}

	pthread_barrier_wait(&barrier_);
	int qpind = tid2qpid_[threadid];
	thread_rc_[threadid].datamr = ibv_reg_mr(qp_rc_[qpind].pd, rdma_buf_[threadid], (size_t)msgsize_*buffperthd_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	assert(thread_rc_[threadid].datamr != NULL);

	//register mem for sync channel
	thread_rc_[threadid].syncmr = ibv_reg_mr(qp_rc_[qpind].pd, (void*)&thread_rc_[threadid].free_buf_id[0][0], MAX_LINKS*MAX_BUFNUM*sizeof(int), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	assert(thread_rc_[threadid].syncmr != NULL);

	pthread_barrier_wait(&barrier_);

	for (int i=0; i<nodenum_; i++) {
		memset(&sync_sge_[threadid][i], 0, sizeof(sync_sge_[threadid][i]));
		sync_sge_[threadid][i].length = sizeof(int);
		sync_sge_[threadid][i].lkey = thread_rc_[threadid].syncmr->lkey;
	}

	if (threadid == 0) {
		for (int j=0; j<threadnum_; j++) {
			for (int i=0; i<nodenum_; i++) {
				local_data_qp_info_[i].buf = (uint64_t)rdma_buf_[j];
				local_data_qp_info_[i].rkey = thread_rc_[j].datamr->rkey;
				local_data_qp_info_[i].size = msgsize_*buffperthd_;
			}
			for (int i=0; i<nodenum_; i++) {
				assert(send(sock_id_[i], &local_data_qp_info_[i], sizeof(local_data_qp_info_[i]), MSG_DONTWAIT) != -1);
			}
			for (int i=0; i<nodenum_; i++) {
				assert(recv(sock_id_[i], &thread_rc_[j].data_qpinfo[i], sizeof(thread_rc_[j].data_qpinfo[i]), MSG_WAITALL) != -1);
			}

			for (int i=0; i<nodenum_; i++) {
				local_data_qp_info_[i].buf = (uint64_t)&thread_rc_[j].free_buf_id[i][0];
				local_data_qp_info_[i].rkey = thread_rc_[j].syncmr->rkey;
				local_data_qp_info_[i].size = sizeof(thread_rc_[j].free_buf_id[i]);
			}
			for (int i=0; i<nodenum_; i++) {
				assert(send(sock_id_[i], &local_data_qp_info_[i], sizeof(local_data_qp_info_[i]), MSG_DONTWAIT) != -1);
			}
			for (int i=0; i<nodenum_; i++) {
				assert(recv(sock_id_[i], &thread_rc_[j].sync_qpinfo[i], sizeof(thread_rc_[j].sync_qpinfo[i]), MSG_WAITALL) != -1);
			}
		}
	}

	pthread_barrier_wait(&barrier_);
	for (int i=0; i<nodenum_; i++) {
		memset(&sync_wr_[threadid][i], 0, sizeof(sync_wr_[threadid][i]));
		sync_wr_[threadid][i].wr_id = threadid*buffperthd_+i;
		sync_wr_[threadid][i].send_flags |= IBV_SEND_SIGNALED;
		sync_wr_[threadid][i].send_flags |= IBV_SEND_INLINE;
		sync_wr_[threadid][i].next = NULL;
		sync_wr_[threadid][i].opcode = IBV_WR_RDMA_WRITE;
		sync_wr_[threadid][i].sg_list = &sync_sge_[threadid][i];
		sync_wr_[threadid][i].num_sge = 1;
		sync_wr_[threadid][i].wr.rdma.rkey = thread_rc_[threadid].sync_qpinfo[i].rkey;
	}

	if (threadid == 0) {
		//sync before start
		for (unsigned int i=0; i<sock_id_.size(); i++) {
			if(sock_id_[i] == -1) {
				continue;
			}
			char temp = 'a';
			assert(send(sock_id_[i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
		}

		for (unsigned int i=0; i<sock_id_.size(); i++) {
			if(sock_id_[i] == -1) {
				continue;
			}
			char temp = 'b';
			assert(recv(sock_id_[i], &temp, sizeof(temp), MSG_WAITALL) != -1);
		}
	}
}

Operator::ResultCode DataSenderShuffleRcReadOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	//assign buffer pages to out_buffer_, put this here because this cost need to be
	//counted in the total cost
	for (int i=0; i<nodenum_; i++) {
		int bufid = GetBufID(threadid);
		assert(bufid != -1);
		//new a page once, then call resetandclear() afterwards
		thread_rc_[threadid].out_buffer[i] = new Page(send_buf_[threadid][bufid].msg, msgsize_-16, NULL, schema.getTupleSize());
		thread_rc_[threadid].out_buffer[i]->clear();
		thread_rc_[threadid].out_buffer_id[i] = bufid;
	}

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderShuffleRcReadOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
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
			dbgassert(bucketspace != NULL);
			schema.copyTuple(bucketspace, tuple);

			//now full, already serialized
			if (thread_rc_[threadid].out_buffer[hashbucket]->canStoreTuple() == false) {
				NotifyConsumer(thread_rc_[threadid].out_buffer_id[hashbucket], hashbucket, MoreData, threadid);
				//now find new buffers
				int bufid = GetBufID(threadid);
				//set page to new buffer
				thread_rc_[threadid].out_buffer[hashbucket]->resetandclear(send_buf_[threadid][bufid].msg, msgsize_-16);
				//to record current bufid used by this out buffer
				thread_rc_[threadid].out_buffer_id[hashbucket] = bufid;
			}
		}
		if (rc == Finished) {
			//send eos and deplete channel
			for (int i=0; i<nodenum_; i++) {
				NotifyConsumer(thread_rc_[threadid].out_buffer_id[i], i, Depleted, threadid);
			}
			return make_pair(Finished, &EmptyPage);
		}

		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}
}

void DataSenderShuffleRcReadOp::threadClose(unsigned short threadid) {
	if (threadid == 0) {
		//shake hands to exit
		for (unsigned int i=0; i<sock_id_.size(); i++) {
			if(sock_id_[i] == -1) {
				continue;
			}
			char temp = 'a';
			assert(send(sock_id_[i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
		}

		for (unsigned int i=0; i<sock_id_.size(); i++) {
			if(sock_id_[i] == -1) {
				continue;
			}
			char temp = 'b';
			assert(recv(sock_id_[i], &temp, sizeof(temp), MSG_WAITALL) != -1);
		}

		//close sockets
		for (unsigned int i=0; i<sock_id_.size(); i++) {
			close(sock_id_[i]);
		}
	}

	pthread_barrier_wait(&barrier_);

	ibv_dereg_mr(thread_rc_[threadid].datamr);
	ibv_dereg_mr(thread_rc_[threadid].syncmr);
	if (threadid == 0) {
		for (int i=0; i<qpnum_; i++) {
			for (int j=0; j<nodenum_; j++) {
				ibv_destroy_qp(qp_rc_[i].qp[j]);
			}
			ibv_destroy_cq(qp_rc_[i].cq);
			ibv_dealloc_pd(qp_rc_[i].pd);
			ibv_close_device(qp_rc_[i].ctx);
		}
	}
	pthread_barrier_wait(&barrier_);
	//free buffer
	numadeallocate(rdma_buf_[threadid]);
	numadeallocate(send_buf_[threadid]);

	for (int i=0; i<MAX_LINKS; i++) {
		if (thread_rc_[threadid].out_buffer[i] != &EmptyPage) {
			delete thread_rc_[threadid].out_buffer[i];
		}
	}
}

void DataSenderShuffleRcReadOp::destroy() {
	TRACE('U');
	char filename[20];
	char nodeID[8];
	snprintf(nodeID, 8, "%d", node_id_);
	strncpy(filename, "rdma_send_mt_", 20);
	strncat(filename, nodeID, 20);
	DUMP(filename);
}

int DataSenderShuffleRcReadOp::GetBufID(int threadid) {
	while (1) {
		for (int i=0; i<nodenum_; i++) {
			if (thread_rc_[threadid].free_buf_id[i][thread_rc_[threadid].localptr[i]] != -1) {
				int bufid = thread_rc_[threadid].free_buf_id[i][thread_rc_[threadid].localptr[i]];
				thread_rc_[threadid].free_buf_id[i][thread_rc_[threadid].localptr[i]] = -1;
				int temp = (thread_rc_[threadid].localptr[i]+1)%MAX_BUFNUM;
				thread_rc_[threadid].localptr[i] = temp;
				return bufid;
			}
		}
	}
	return -1;
}

int DataSenderShuffleRcReadOp::NotifyConsumer(int bufid, unsigned int dest_id, deplete_t deplete, int threadid) {
	int qpind = tid2qpid_[threadid];
	Page *nsendPage = thread_rc_[threadid].out_buffer[dest_id];

	int buflen = nsendPage->getUsedSpace();
	send_buf_[threadid][bufid].datalen = buflen;
	send_buf_[threadid][bufid].nodeid = node_id_;
	send_buf_[threadid][bufid].deplete = deplete;
	//this buf id should contain the thread info
	send_buf_[threadid][bufid].dest_bufid = threadid*buffperthd_+bufid;

	send_buf_[threadid][bufid].serialize();

	sync_sge_[threadid][dest_id].addr = (uint64_t)&bufid;
	int curptr = thread_rc_[threadid].remoteptr[dest_id];
	thread_rc_[threadid].remoteptr[dest_id]++;
	thread_rc_[threadid].remoteptr[dest_id] %= MAX_BUFNUM;
	sync_wr_[threadid][dest_id].wr.rdma.remote_addr = thread_rc_[threadid].sync_qpinfo[dest_id].buf + curptr*sizeof(int);
	struct ibv_send_wr *bad_wr;
	int pollcomp = 0;
	pollcomp = ibv_poll_cq(qp_rc_[qpind].cq, 1024, sync_wc_[threadid]);
	assert(pollcomp >= 0);
	if (pollcomp > 0) {
		for (int i=0; i<pollcomp; i++) {
			assert(sync_wc_[threadid][i].status == IBV_WC_SUCCESS);
		}
	}
	assert(ibv_post_send(qp_rc_[qpind].qp[dest_id], &sync_wr_[threadid][dest_id], &bad_wr) == 0);
	return 0;
}
