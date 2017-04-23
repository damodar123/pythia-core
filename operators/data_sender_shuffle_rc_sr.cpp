
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
#include <stdlib.h>

#include <iostream>
#include <cstdlib>

#include <iomanip>

#include "../rdtsc.h"

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
union TraceEntry
{
	struct
	{
		unsigned char garbage[7];
		unsigned char label;
	};
	unsigned long long tick;
};

static const unsigned long TraceLogSize = 0x1000000;
static const unsigned long TraceMaxThreads = 32;
//static TraceEntry TraceLog[TraceLogSize*TraceMaxThreads];
static TraceEntry *TraceLog;
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

//to build Rdma connection, RC, one QP for each thread
int DataSenderShuffleRcSrOp::RdmaConnect(int qpind) {
	int ret = -1;

	int numdev;
	struct ibv_device **ibdevlist = ibv_get_device_list(&numdev);

	if (ibname_.empty() == false)
	{
		for (int i=0; i<numdev; i++)
		{
			if (strcmp(ibv_get_device_name(ibdevlist[i]), ibname_.c_str()) == 0)
			{
				qp_rc_[qpind].ctx = ibv_open_device(ibdevlist[i]);
				break;
			}
		}
	}
	else
	{
		qp_rc_[qpind].ctx = ibv_open_device(ibdevlist[0]);
	}
	assert(qp_rc_[qpind].ctx != NULL);

	ibv_free_device_list(ibdevlist);
	qp_rc_[qpind].cq = ibv_create_cq(qp_rc_[qpind].ctx, buffnum_/qpnum_+1, NULL, NULL, 0);

	for (unsigned int i = 0; i<sock_id_.size(); i++) {
		qp_rc_[qpind].pd[i] = ibv_alloc_pd(qp_rc_[qpind].ctx);
		struct ibv_qp_init_attr qp_init_attr;
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.send_cq = qp_rc_[qpind].cq;
		qp_init_attr.recv_cq = qp_rc_[qpind].cq;
		qp_init_attr.cap.max_send_wr  = buffnum_/qpnum_;
		qp_init_attr.cap.max_recv_wr  = 1;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 0;
		qp_init_attr.qp_type = IBV_QPT_RC;

		qp_rc_[qpind].qp[i] = ibv_create_qp(qp_rc_[qpind].pd[i], &qp_init_attr);
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state   = IBV_QPS_INIT;
		qp_attr.pkey_index   = 0;
		qp_attr.port_num   = 1;
		qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

		ret = ibv_modify_qp(qp_rc_[qpind].qp[i], &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
		assert(ret == 0);
		struct ibv_port_attr ptattr;
		ibv_query_port(qp_rc_[qpind].ctx, 1, &ptattr);

		udinfo[i].lid = ptattr.lid;
		udinfo[i].qpn = qp_rc_[qpind].qp[i]->qp_num;
		udinfo[i].psn = rand() & 0xffffff;
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			assert(send(sock_id_[i], &udinfo[i], sizeof(udinfo[i]), MSG_DONTWAIT) != -1);
		}
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			assert(recv(sock_id_[i], &remoteqp_[i], sizeof(remoteqp_[i]), MSG_WAITALL) != -1);
		}
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		assert(sock_id_[i] != -1);

		struct ibv_qp_attr conn_attr;
		memset(&conn_attr, 0, sizeof(conn_attr));
		conn_attr.qp_state     = IBV_QPS_RTR;
		conn_attr.path_mtu     = IBV_MTU_4096;
		conn_attr.dest_qp_num    = remoteqp_[i].qpn;
		conn_attr.rq_psn       = remoteqp_[i].psn;
		conn_attr.ah_attr.is_global      = 0;
		conn_attr.ah_attr.dlid       = remoteqp_[i].lid;
		conn_attr.ah_attr.sl         = 0;
		conn_attr.ah_attr.src_path_bits    = 0;
		conn_attr.ah_attr.port_num     = 1;
		conn_attr.max_dest_rd_atomic     = 0;

		int rtr_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN
		                | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
		ret = ibv_modify_qp(qp_rc_[qpind].qp[i], &conn_attr, rtr_flags);
		assert(ret == 0);

		memset(&conn_attr, 0, sizeof(conn_attr));
		conn_attr.qp_state      = IBV_QPS_RTS;
		conn_attr.sq_psn      = udinfo[i].psn;
		conn_attr.timeout = 14; //consider to increase this when scale is large
		conn_attr.retry_cnt = 12;
		conn_attr.rnr_retry = 7;
		conn_attr.path_mig_state = IBV_MIG_REARM;
		conn_attr.max_rd_atomic = 2;

		int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT
		                | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

		ret = ibv_modify_qp(qp_rc_[qpind].qp[i], &conn_attr, rts_flags);
		assert(ret == 0);
	}
	return 0;
}

//to build Rdma uc connection for sync
void DataSenderShuffleRcSrOp::RdmaWriteConnect(credit_t *credit_array) {
	int numdev;
	struct ibv_device **ibdevlist = ibv_get_device_list(&numdev);

	if (ibname_.empty() == false)
	{
		for (int i=0; i<numdev; i++)
		{
			if (strcmp(ibv_get_device_name(ibdevlist[i]), ibname_.c_str()) == 0)
			{
				for (int j=0; j<nodenum_; j++)
				{
					sync_ctx_[j] = ibv_open_device(ibdevlist[i]);
				}
				break;
			}
		}
	}
	else
	{
		for (int j=0; j<nodenum_; j++)
		{
			sync_ctx_[j] = ibv_open_device(ibdevlist[0]);
		}
	}

	for (int i=0; i<nodenum_; i++)
	{
		assert(sync_ctx_[i] != NULL);
	}

	ibv_free_device_list(ibdevlist);
	for (unsigned int i = 0; i<sock_id_.size(); i++) {
		sync_pd_[i] = ibv_alloc_pd(sync_ctx_[i]);
		sync_cq_[i] = ibv_create_cq(sync_ctx_[i], MAX_QP_DEPTH+1, NULL, NULL, 0);

		struct ibv_qp_init_attr qp_init_attr;
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.send_cq = sync_cq_[i];
		qp_init_attr.recv_cq = sync_cq_[i];
		qp_init_attr.cap.max_send_wr  = 1;
		qp_init_attr.cap.max_recv_wr  = MAX_QP_DEPTH;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 16;
		qp_init_attr.qp_type = IBV_QPT_UC;

		sync_qp_[i] = ibv_create_qp(sync_pd_[i], &qp_init_attr);
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state   = IBV_QPS_INIT;
		qp_attr.pkey_index   = 0;
		qp_attr.port_num   = 1;
		qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

		ibv_modify_qp(sync_qp_[i], &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
		sync_sendmr_[i] = ibv_reg_mr(sync_pd_[i], (void*)&credit_array[i*MAX_THREADS], sizeof(credit_array[i])*MAX_THREADS, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
		struct ibv_port_attr ptattr;
		ibv_query_port(sync_ctx_[i], 1, &ptattr);

		rdma_write_ucinfo_[i].lid = ptattr.lid;
		rdma_write_ucinfo_[i].qpn = sync_qp_[i]->qp_num;
		rdma_write_ucinfo_[i].psn = rand() & 0xffffff;
		rdma_write_ucinfo_[i].buf = (uint64_t)&credit_array[i*MAX_THREADS];
		rdma_write_ucinfo_[i].rkey = sync_sendmr_[i]->rkey;
		rdma_write_ucinfo_[i].size = sizeof(credit_array[i])*MAX_THREADS;
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			send(sock_id_[i], &rdma_write_ucinfo_[i], sizeof(rdma_write_ucinfo_[i]), MSG_DONTWAIT);
		}
	}

	for (unsigned int i=0; i<sock_id_.size(); i++) {
		if (sock_id_[i] != -1) {
			recv(sock_id_[i], &rdma_write_qp_info_[i], sizeof(rdma_write_qp_info_[i]), MSG_WAITALL);

			struct ibv_qp_attr conn_attr;
			memset(&conn_attr, 0, sizeof(conn_attr));
			conn_attr.qp_state     = IBV_QPS_RTR;
			conn_attr.path_mtu     = IBV_MTU_4096;
			conn_attr.dest_qp_num    = rdma_write_qp_info_[i].qpn;
			conn_attr.rq_psn       = rdma_write_qp_info_[i].psn;
			conn_attr.ah_attr.is_global      = 0;
			conn_attr.ah_attr.dlid       = rdma_write_qp_info_[i].lid;
			conn_attr.ah_attr.sl         = 0;
			conn_attr.ah_attr.src_path_bits    = 0;
			conn_attr.ah_attr.port_num     = 1;

			int rtr_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
			ibv_modify_qp(sync_qp_[i], &conn_attr, rtr_flags);

			memset(&conn_attr, 0, sizeof(conn_attr));
			conn_attr.qp_state      = IBV_QPS_RTS;
			conn_attr.sq_psn      = rdma_write_ucinfo_[i].psn;
			int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

			ibv_modify_qp(sync_qp_[i], &conn_attr, rts_flags);
		}
	}
}

//input attributes for rdma send:
// int port: port used for TCP/IP to do sync
// int msgsize: send message size, 4096 bytes by default
// int buffnum: number of buffers used in send

/*
 * @param msgsize The size of one message sent out
 * @param buffnum The total number of buffers for this operator
 * @param threadnum The total number of threads in the operator
 * @param qpnum The number of QPs used in this operator, currently only support
 * 1 or equal to the number of threads
 * @param opid The id of the operator, the operators which belongs to the same
 * communication group should have the same id. Operators which do not belong to the same communication group should not use the same id.
 * @param ibname The name of the network card to be used
 * @param nodeid The id of the node running this operator
 * @param hostIP The IP address of the machine running this operator
 * @param destIP The IP address of the machines which the operator communicates with
 */
void DataSenderShuffleRcSrOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	static_assert(sizeof(char) == 1);

	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	hashfn = TupleHasher::create(schema, cfg["hash"]);
	msgsize_ = cfg["msgsize"];
	buffnum_ = cfg["buffnum"];
	if (cfg.exists("threadnum")) {
		threadnum_ = cfg["threadnum"];
	}
	else {
		threadnum_ = 1;
	}
	if (cfg.exists("qpnum")) {
		qpnum_ = cfg["qpnum"];
	}
	else {
		qpnum_ = 1;
	}
	if (cfg.exists("opid"))
	{
		operator_id_ = cfg["opid"];
	}
	else
	{
		operator_id_ = 0;
	}

	assert(buffnum_/threadnum_ <= MAX_QP_DEPTH);
	nodenum_ = cfg["nodenum"];
	buffperthd_ = buffnum_/threadnum_;
	assert(nodenum_ < MAX_LINKS);
	assert(buffnum_/threadnum_>=nodenum_);

	if (cfg.exists("ibname"))
	{
		ibname_ = (const char*)cfg["ibname"];
	}

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
	for (int i=0; i<size; i++) {
		sock_id_.push_back(-1);
	}

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].out_buffer[j] = &EmptyPage;
		}
	}
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].out_buffer_id[j] = -1;
		}
	}
	for (int i=0; i<MAX_THREADS; i++) {
		thread_rc_[i].initial_buf_cnt = 0;
		for (int j=0; j<MAX_BUFF_NUM; j++) {
			thread_rc_[i].initial_buf_id[j] = -1;
		}
	}
	for (int i=0; i<MAX_LINKS; i++) {
		for (int threadid=0; threadid < MAX_THREADS; threadid++) {
			credit_[i*MAX_THREADS+threadid].credit = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++)
	{
		for (int j=0; j<MAX_LINKS; j++)
		{
			qp_rc_[i].pd[j] = NULL;
		}
	}

	//populate rdma structs for sync channel
	//one channel for each destination
	for (int i=0; i<size; i++) {
		sync_cq_.push_back(NULL);
	}
	for (int i=0; i<size; i++) {
		sync_qp_.push_back(NULL);
	}
	for (int i=0; i<size; i++) {
		sync_sendmr_.push_back(NULL);
	}
	for (int i=0; i<size; i++) {
		sync_ctx_.push_back(NULL);
	}
	for (int i=0; i<size; i++) {
		sync_pd_.push_back(NULL);
	}

	for (int i=0; i<threadnum_; i++) {
		tid2qpid_[i] = i*qpnum_/threadnum_;
	}
	pthread_barrier_init(&barrier_, NULL, threadnum_);

#ifdef TRACESENDLOG
	TraceLog = (TraceEntry*)numaallocate_local("TRLG", sizeof(TraceEntry)*TraceLogSize*TraceMaxThreads, this);
#endif
}


void DataSenderShuffleRcSrOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	//create buffers for rdma
	rdma_buf_[threadid] = (char*)numaallocate_local("DSbf", (size_t)msgsize_*buffperthd_, this);
	memset(rdma_buf_[threadid], 0, (size_t)msgsize_*buffperthd_);

	//create memory space for the struct
	send_buf_[threadid] = (struct RdmaBuf *)numaallocate_local("DSst", sizeof(struct RdmaBuf)*buffperthd_, this);
	memset(send_buf_[threadid], 0, sizeof(struct RdmaBuf)*buffperthd_);

	//associate each RdmaBuf with its corresponding buffer
	for (int i=0; i<buffperthd_; i++) {
		send_buf_[threadid][i].set((char*)rdma_buf_[threadid]+i*msgsize_);
	}

	//initialize the available buff queue with buff id
	thread_rc_[threadid].initial_buf_cnt = 0;
	for (int i=0; i<buffperthd_; i++) {
		thread_rc_[threadid].initial_buf_id[i] = i;
		thread_rc_[threadid].initial_buf_cnt++;
	}

	//build connection here
	if (threadid == 0) {
		TcpServer(LISTEN_PORT+operator_id_, host_ip_.c_str(), nodenum_, sock_id_);

		for (unsigned int i=0; i<port_.size(); i++) {
			assert(sock_id_[i] != -1);
		}

		assert(sizeof(credit_t) == 64);
	};

	assert(rdma_buf_[threadid] != NULL);

	if (threadid == 0) {
		for (int i=0; i<qpnum_; i++) {
			RdmaConnect(i);
		}

		RdmaWriteConnect(credit_);
	}

	//barrier to wait for all these allocation completes
	pthread_barrier_wait(&barrier_);

	int qpind = tid2qpid_[threadid];
	for (int i=0; i<nodenum_; i++) {
		assert(qp_rc_[qpind].pd[i] != NULL);
		qp_rc_[qpind].sendmr[i][threadid] = ibv_reg_mr(qp_rc_[qpind].pd[i], rdma_buf_[threadid], (size_t)msgsize_*buffperthd_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
		assert(qp_rc_[qpind].sendmr[i][threadid] != NULL);
	}

	//populate send_sge_, put it here because it needs the value of local mr
	for (int i=0; i<qpnum_; i++) {
		for (int j=0; j<nodenum_; j++) {
			memset(&qp_rc_[i].send_sge[j][threadid], 0, sizeof(struct ibv_sge));
			qp_rc_[i].send_sge[j][threadid].length = msgsize_;
		}
	}

	for (int i=0; i<qpnum_; i++) {
		for (int j=0; j<nodenum_; j++) {
			memset(&qp_rc_[i].send_wr[j][threadid], 0, sizeof(struct ibv_send_wr));
			qp_rc_[i].send_wr[j][threadid].send_flags = IBV_SEND_SIGNALED;
			qp_rc_[i].send_wr[j][threadid].next = NULL;
			qp_rc_[i].send_wr[j][threadid].opcode = IBV_WR_SEND;
			qp_rc_[i].send_wr[j][threadid].sg_list = &qp_rc_[i].send_sge[j][threadid];
			qp_rc_[i].send_wr[j][threadid].num_sge = 1;
		}
	}

	if (threadid == 0) {
		//sync to exit
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

Operator::ResultCode DataSenderShuffleRcSrOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	for (int i=0; i<nodenum_; i++) {
		int bufid;
		thread_rc_[threadid].initial_buf_cnt--;
		assert(thread_rc_[threadid].initial_buf_cnt >= 0);
		bufid = thread_rc_[threadid].initial_buf_id[thread_rc_[threadid].initial_buf_cnt];
		//new a page once, then call resetandclear() afterwards
		thread_rc_[threadid].out_buffer[i] = new Page(send_buf_[threadid][bufid].msg, msgsize_-16, NULL, schema.getTupleSize());
		thread_rc_[threadid].out_buffer[i]->clear();
		thread_rc_[threadid].out_buffer_id[i] = threadid*buffperthd_+bufid;
	}

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderShuffleRcSrOp::getNext(unsigned short threadid)
{
	void *tuple;
	unsigned int hashbucket;

	Operator::GetNextResultT result;
	Operator::ResultCode rc = Ready;
	Page* in = &EmptyPage;
	int tupoffset = 0;

	while (rc == Ready)
	{

		//here we assume a one to one mapping between rdma wr_id and send buff
		//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;

		if (rc == Error)
		{
			break;
		}

		while ((tuple = in->getTupleOffset(tupoffset++)) != NULL)
		{
			//hash to decide destination
			hashbucket = hashfn.hash(tuple);
			void * bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();

			// If bucket full, send and find new buffer.
			//
			if (bucketspace == NULL)
			{
				RdmaSendMoreData(threadid, thread_rc_[threadid].out_buffer_id[hashbucket], hashbucket);

				int bufid = -1;

				//get buffer from queue
				if (thread_rc_[threadid].initial_buf_cnt >= 1) {
					thread_rc_[threadid].initial_buf_cnt--;
					bufid = threadid*buffperthd_+thread_rc_[threadid].initial_buf_id[thread_rc_[threadid].initial_buf_cnt];
					assert(bufid != -1);
				}
				else {
					int comp = -1;
					do {
						comp = ibv_poll_cq(qp_rc_[threadid*qpnum_/threadnum_].cq, 1, &thread_rc_[threadid].send_wc);
						assert(comp >= 0);
						if (comp != 0) {
							bufid = thread_rc_[threadid].send_wc.wr_id;
							assert(thread_rc_[threadid].send_wc.status == IBV_WC_SUCCESS);
						}
					} while (bufid == -1);
				}

				assert(bufid != -1);
				thread_rc_[threadid].out_buffer[hashbucket]->resetandclear(send_buf_[bufid/buffperthd_][bufid%buffperthd_].msg, msgsize_-16);
				//to record current bufid used by this out buffer
				thread_rc_[threadid].out_buffer_id[hashbucket] = bufid;
				bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();
			}

			dbgassert(bucketspace != NULL);
			assert(bucketspace != NULL);
			schema.copyTuple(bucketspace, tuple);
		}
	}

	assert(rc != Error);


	for (int i=0; i<nodenum_; i++) {
		RdmaSendDepleted(threadid, thread_rc_[threadid].out_buffer_id[i], i);
	}

	return make_pair(Finished, &EmptyPage);
}

void DataSenderShuffleRcSrOp::threadClose(unsigned short threadid) {

	//shake hands to exit
	//block until all destinations receive all the data
	if (threadid == 0) {
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

	for (int i=0; i<nodenum_; i++) {
		ibv_dereg_mr(qp_rc_[tid2qpid_[threadid]].sendmr[i][threadid]);
	}

	pthread_barrier_wait(&barrier_);

	if (threadid == 0) {
		for (int i=0; i<qpnum_; i++) {
			for (int j=0; j<nodenum_; j++) {
				ibv_destroy_qp(qp_rc_[i].qp[j]);
				ibv_dealloc_pd(qp_rc_[i].pd[j]);
			}
		}

		for (int i=0; i<qpnum_; i++) {
			ibv_destroy_cq(qp_rc_[i].cq);
			ibv_close_device(qp_rc_[i].ctx);
		}
	}

	pthread_barrier_wait(&barrier_);

	numadeallocate(rdma_buf_[threadid]);
	numadeallocate(send_buf_[threadid]);

	if (threadid == 0) {
		for (unsigned int i=0; i<sync_sendmr_.size(); i++) {
			ibv_dereg_mr(sync_sendmr_[i]);
		}

		for (unsigned int i=0; i<sync_qp_.size(); i++) {
			ibv_destroy_qp(sync_qp_[i]);
		}

		for (unsigned int i=0; i<sync_cq_.size(); i++) {
			ibv_destroy_cq(sync_cq_[i]);
		}

		for (unsigned int i=0; i<sync_pd_.size(); i++) {
			ibv_dealloc_pd(sync_pd_[i]);
		}

		for (unsigned int i=0; i<sync_ctx_.size(); i++) {
			ibv_close_device(sync_ctx_[i]);
		}
	}

	//delete Page in out_buffer_
	for (int i=0; i<MAX_LINKS; i++) {
		if (thread_rc_[threadid].out_buffer[i] != &EmptyPage) {
			delete thread_rc_[threadid].out_buffer[i];
		}
	}
}

void DataSenderShuffleRcSrOp::destroy() {
	TRACE('U');  // U means destroy() called
	char filename[20];
	char nodeID[8];
	snprintf(nodeID, 8, "%d", node_id_);
	strncpy(filename, "rdma_send_mt_", 20);
	strncat(filename, nodeID, 20);
	DUMP(filename);
}

int DataSenderShuffleRcSrOp::RdmaSendDepleted(int threadid, int bufid, unsigned int dest_id)
{
	assert(bufid != -1);
	Page *nsendPage = thread_rc_[threadid].out_buffer[dest_id];
	int sendbuf_ind1 = bufid/buffperthd_;
	int sendbuf_ind2 = bufid%buffperthd_;

	int buflen = nsendPage->getUsedSpace();
	send_buf_[sendbuf_ind1][sendbuf_ind2].datalen = buflen;
	send_buf_[sendbuf_ind1][sendbuf_ind2].nodeid = node_id_;
	//this should be set in RdmaSend, we want to make sure that the deplete data
	//is sent as the last packet
	send_buf_[sendbuf_ind1][sendbuf_ind2].deplete = Depleted;
	send_buf_[sendbuf_ind1][sendbuf_ind2].destid  = dest_id;

	send_buf_[sendbuf_ind1][sendbuf_ind2].serialize();

	int ret = 0;

	//trying to send this buffer
	while (attemptToGetCredit(dest_id, threadid) == false)
	{
	}
	PostSend(dest_id, bufid, threadid);
	ret++;

	return ret;
}

int DataSenderShuffleRcSrOp::RdmaSendMoreData(int threadid, int bufid, unsigned int dest_id)
{
	assert(bufid != -1);
	Page *nsendPage = thread_rc_[threadid].out_buffer[dest_id];
	int sendbuf_ind1 = bufid/buffperthd_;
	int sendbuf_ind2 = bufid%buffperthd_;

	int buflen = nsendPage->getUsedSpace();
	send_buf_[sendbuf_ind1][sendbuf_ind2].datalen = buflen;
	send_buf_[sendbuf_ind1][sendbuf_ind2].nodeid = node_id_;
	//this should be set in RdmaSend, we want to make sure that the deplete data
	//is sent as the last packet
	send_buf_[sendbuf_ind1][sendbuf_ind2].deplete = MoreData;
	send_buf_[sendbuf_ind1][sendbuf_ind2].destid  = dest_id;
	send_buf_[sendbuf_ind1][sendbuf_ind2].serialize();

	int ret = 0;

	//trying to send this buffer
	while (attemptToGetCredit(dest_id, threadid) == false)
	{
	}
	PostSend(dest_id, bufid, threadid);

	return ret;
}

//inline function for post rdma send
inline int DataSenderShuffleRcSrOp::PostSend(int dest_id, int bufid, int threadid)
{
	int qpind = tid2qpid_[threadid];
	qp_rc_[qpind].send_sge[dest_id][threadid].lkey = qp_rc_[qpind].sendmr[dest_id][bufid/buffperthd_]->lkey;
	qp_rc_[qpind].send_sge[dest_id][threadid].addr = (uint64_t) send_buf_[bufid/buffperthd_][bufid%buffperthd_].buffaddr();
	//set next ibv_send_wr to be NULL
	qp_rc_[qpind].send_wr[dest_id][threadid].wr_id = bufid;
	struct ibv_send_wr *bad_wr;

	int ret = 0;
	ret = ibv_post_send(qp_rc_[qpind].qp[dest_id], &qp_rc_[qpind].send_wr[dest_id][threadid], &bad_wr);
	assert(ret == 0);
	return ret;
}

//attemp to get credit, return true if succeed
//the thread id is just for tracing
inline bool DataSenderShuffleRcSrOp::attemptToGetCredit(int dest_id, int threadid)
{

	int qpind = tid2qpid_[threadid];
	int newcredit = qp_rc_[qpind].local_credit[dest_id].credit;
	int oldcredit;

	bool ret = false;

	do {
		if (newcredit >= credit_[dest_id*MAX_THREADS+qpind].credit)
		{
			goto exit;
		}
		oldcredit = newcredit;
		newcredit = oldcredit + 1;
		newcredit = atomic_compare_and_swap(&qp_rc_[qpind].local_credit[dest_id].credit, oldcredit, newcredit);
	} while(oldcredit != newcredit);

	ret = true;

exit:
	return ret;
}
