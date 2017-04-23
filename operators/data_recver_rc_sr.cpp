
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

//to build Rdma connection
int DataRecverRcSrOp::RdmaConnect(int qpind) {
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
		qp_init_attr.cap.max_send_wr  = 1;
		qp_init_attr.cap.max_recv_wr  = buffnum_/qpnum_;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 0;
		qp_init_attr.qp_type = IBV_QPT_RC;

		qp_rc_[qpind].qp[i] = ibv_create_qp(qp_rc_[qpind].pd[i], &qp_init_attr);
		assert(qp_rc_[qpind].qp[i] != NULL);
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
		assert(sock_id_[i] != -1);
		assert(recv(sock_id_[i], &remoteqp_[i], sizeof(remoteqp_[i]), MSG_WAITALL) != -1);
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

		int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT
		                | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

		ret = ibv_modify_qp(qp_rc_[qpind].qp[i], &conn_attr, rts_flags);
		assert(ret == 0);
	}
	return 0;
}

//to build Rdma connection
void DataRecverRcSrOp::RdmaWriteConnect(credit_t credit_array[]) {
	int ret = -1;

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
		qp_init_attr.cap.max_send_wr  = MAX_QP_DEPTH;
		qp_init_attr.cap.max_recv_wr  = 1;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 16;
		qp_init_attr.qp_type = IBV_QPT_UC;

		sync_qp_[i] = ibv_create_qp(sync_pd_[i], &qp_init_attr);
		assert(sync_qp_[i] != NULL);
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state   = IBV_QPS_INIT;
		qp_attr.pkey_index   = 0;
		qp_attr.port_num   = 1;
		qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

		ret = ibv_modify_qp(sync_qp_[i], &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
		assert(ret == 0);
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
			ret = ibv_modify_qp(sync_qp_[i], &conn_attr, rtr_flags);
			assert(ret == 0);

			memset(&conn_attr, 0, sizeof(conn_attr));
			conn_attr.qp_state      = IBV_QPS_RTS;
			conn_attr.sq_psn      = rdma_write_ucinfo_[i].psn;
			int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

			ret = ibv_modify_qp(sync_qp_[i], &conn_attr, rts_flags);
			assert(ret == 0);
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
 * communication group should have the same id. Operators which do not belong to the same 
 * communication group should not use the same id
 * @param creditthd How frequent the receiver update the credit in the sender
 * @param ibname The name of the network card to be used
 * @param nodeid The id of the node running this operator
 * @param hostIP The IP address of the machine running this operator
 * @param destIP The IP address of the machines which the operator communicates with
 */

void DataRecverRcSrOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	static_assert(sizeof(char) == 1);

	Operator::init(root, cfg);
	schema = Schema::create(cfg["schema"]);
	msgsize_ = cfg["msgsize"];
	buffnum_ = cfg["buffnum"];
	nodenum_ = cfg["nodenum"];
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

	buffperthd_ = buffnum_/threadnum_;
	if (cfg.exists("creditthd")) {
		credit_writeback_thd_ = cfg["creditthd"];
	}
	else {
		credit_writeback_thd_ = 1;
	}
	if (cfg.exists("ibname"))
	{
		ibname_ = (const char*)cfg["ibname"];
	}

	assert(buffnum_/threadnum_>=nodenum_);
	assert(buffnum_/threadnum_<=MAX_QP_DEPTH);

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
	for (int i=0; i<MAX_LINKS; i++) {
		for (int j=0; j<MAX_THREADS; j++) {
			credit_[i*MAX_THREADS+j].credit = -1;
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

	for (int i=0; i<MAX_THREADS; i++) {
		tid2qpid_[i] = i*qpnum_/threadnum_;
	}

	for (int i=0; i<qpnum_; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			qp_rc_[i].deplete_cnt[j] = threadnum_/qpnum_;
			qp_rc_[i].deplete[j] = false;
		}
		qp_rc_[i].eos = false;
	}

	pthread_barrier_init(&barrier_, NULL, threadnum_);

#ifdef TRACESENDLOG
	TraceLog = (TraceEntry*)numaallocate_local("TRLG", sizeof(TraceEntry)*TraceLogSize*TraceMaxThreads, this);
#endif
}

void DataRecverRcSrOp::threadInit(unsigned short threadid)
{
	assert(threadid < threadnum_);
	//buffer used for rdma recv
	rdma_buf_[threadid] = (char*)numaallocate_local("DRbf", (size_t)msgsize_*buffperthd_, this);
	memset(rdma_buf_[threadid], 0, (size_t)msgsize_*buffperthd_);

	recv_buf_[threadid] = (RdmaBuf *)numaallocate_local("DRst", sizeof(RdmaBuf)*buffperthd_, this);
	memset(recv_buf_[threadid], 0, sizeof(RdmaBuf)*buffperthd_);

	//associate each RdmaBuf with its corresponding buffer
	for (int i=0; i<buffperthd_; i++) {
		recv_buf_[threadid][i].set((char*)rdma_buf_[threadid]+i*msgsize_);
	}

	if (threadid == 0) {
		//build connection here
		TcpClient(LISTEN_PORT+operator_id_, dest_ip_, nodenum_, sock_id_, node_id_);

		for (unsigned int i=0; i< sock_id_.size(); i++) {
			if (sock_id_[i] != -1) {
				for (int j=0; j<threadnum_; j++) {
					credit_[i*MAX_THREADS+j].credit = 0;
				}
			}
		}
	}
	//rdma connect for data channel
	assert(rdma_buf_[threadid] != NULL);
	if (threadid == 0) {
		for (int i=0; i<qpnum_; i++) {
			RdmaConnect(i);
		}

		//rdma connect for sync channel
		//wait until all threads execute RdmaConnect
		RdmaWriteConnect(credit_);
		for (int i=0; i<nodenum_; i++) {
			for (int j=0; j<qpnum_; j++) {
				sync_sge_[i*MAX_THREADS+j].addr = (uint64_t)&(credit_[i*MAX_THREADS+j].credit);
				sync_sge_[i*MAX_THREADS+j].length = sizeof(credit_[i*MAX_THREADS+j].credit);
				sync_sge_[i*MAX_THREADS+j].lkey = sync_sendmr_[i]->lkey;
			}
		}
		for (int i=0; i<nodenum_; i++) {
			for (int j=0; j<qpnum_; j++) {
				memset(&sync_wr_[i*MAX_THREADS+j], 0, sizeof(sync_wr_[i*MAX_THREADS+j]));
				sync_wr_[i*MAX_THREADS+j].wr.rdma.remote_addr = rdma_write_qp_info_[i].buf+j*sizeof(credit_t);
				sync_wr_[i*MAX_THREADS+j].wr.rdma.rkey = rdma_write_qp_info_[i].rkey;
				sync_wr_[i*MAX_THREADS+j].send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
				sync_wr_[i*MAX_THREADS+j].wr_id = i*MAX_THREADS+j;
				sync_wr_[i*MAX_THREADS+j].next = NULL;
				sync_wr_[i*MAX_THREADS+j].opcode = IBV_WR_RDMA_WRITE;
				sync_wr_[i*MAX_THREADS+j].sg_list = &sync_sge_[i*MAX_THREADS+j];
				sync_wr_[i*MAX_THREADS+j].num_sge = 1;
			}
		}
	}

	//barrier to wait for all these allocation completes
	pthread_barrier_wait(&barrier_);

	int qpind = threadid*qpnum_/threadnum_;
	for (int i=0; i<nodenum_; i++) {
		assert(qp_rc_[qpind].pd[i] != NULL);
		qp_rc_[qpind].sendmr[i][threadid] = ibv_reg_mr(qp_rc_[qpind].pd[i], rdma_buf_[threadid], (size_t)msgsize_*buffperthd_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
		assert(qp_rc_[qpind].sendmr[i][threadid] != NULL);
	}

	//buffer of data to up operators
	void *space = numaallocate_local("DRpg", sizeof(Page), this);
	thread_rc_[threadid].output = new (space) Page(buffsize, schema.getTupleSize(), this, "DRpg");

	//allocate a Page which can only only one tuple (because it will be checked in buffer.h)
	thread_rc_[threadid].inter_buff.buff = new Page(schema.getTupleSize(), schema.getTupleSize(), this, "DRib");
	thread_rc_[threadid].inter_buff.bufind = -1;
	thread_rc_[threadid].inter_buff.deplete = 0;

	//populate recv sge and recv wr
	for (int i=0; i<qpnum_; i++) {
		for (int j=0; j<nodenum_; j++) {
			memset(&qp_rc_[i].recv_sge[j][threadid], 0, sizeof(struct ibv_sge));
			qp_rc_[i].recv_sge[j][threadid].length = msgsize_;
		}
	}

	for (int i=0; i<qpnum_; i++) {
		for (int j=0; j<nodenum_; j++) {
			memset(&qp_rc_[i].recv_wr[j][threadid], 0, sizeof(struct ibv_recv_wr));
			qp_rc_[i].recv_wr[j][threadid].next = NULL;
			qp_rc_[i].recv_wr[j][threadid].sg_list = &qp_rc_[i].recv_sge[j][threadid];
			qp_rc_[i].recv_wr[j][threadid].num_sge = 1;
		}
	}

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
	}
}

Operator::ResultCode DataRecverRcSrOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode = Operator::Ready;
	for (int i=0; i<buffnum_/threadnum_; i++) {
		RdmaRecv(threadid*buffperthd_+i, i%nodenum_, threadid);
	}
	return rescode;
}

Operator::GetNextResultT DataRecverRcSrOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
	//poll for recv completion
	void *tuple;
	int qpind = threadid*qpnum_/threadnum_;
	thread_rc_[threadid].output->clear();
	while (1) {
		while ((tuple = (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr))) {
			//output large enough to host all data in thread_rc_[threadid].inter_buff
			size_t leftdata_in_interbuff = thread_rc_[threadid].inter_buff.leftdata();
			if (thread_rc_[threadid].output->canStore(leftdata_in_interbuff)) {
				void *bucketspace = thread_rc_[threadid].output->allocate(leftdata_in_interbuff);
				memcpy(bucketspace, (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr), leftdata_in_interbuff);
				//used up data in thread_rc_[threadid].inter_buff, post the buffer for recv, break
				RdmaRecv(thread_rc_[threadid].inter_buff.bufind, thread_rc_[threadid].inter_buff.recv_node, threadid);
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
		if (thread_rc_[threadid].inter_buff.deplete == 1) {
			if (atomic_increment(&qp_rc_[qpind].deplete_cnt[thread_rc_[threadid].inter_buff.recv_node], -1) == 1) {
				qp_rc_[qpind].deplete[thread_rc_[threadid].inter_buff.recv_node] = true;
			}
			for (int i = 0; i<nodenum_; i++) {
				//some channel not eos
				if (qp_rc_[qpind].deplete[i] == false) {
					break;
				}
				//all channel eos
				if (i == (nodenum_-1)) {
					qp_rc_[qpind].eos = true;
					return make_pair(Finished, thread_rc_[threadid].output);
				}
			}
		}

		//poll for one completion
		int recvcomp = 0;
		int pollcomp_cnt = 0;

		int qpind = tid2qpid_[threadid];
		do
		{
			recvcomp = ibv_poll_cq(qp_rc_[qpind].cq, 1, &thread_rc_[threadid].recv_comp);
			assert(thread_rc_[threadid].recv_comp.status == IBV_WC_SUCCESS);
			assert(recvcomp >= 0);
			pollcomp_cnt++;
			if (pollcomp_cnt % (1024*1024)==0) {
				//if spend too much time on poll, write credit to remote node again
				for (int i=0; i<nodenum_; i++) {
					// if this src not depleted, then keep writing credit
					if (qp_rc_[tid2qpid_[threadid]].deplete[i] == true) {
						continue;
					}
					RdmaWrite(i, threadid);
				}
			}
		} while ((recvcomp == 0) && (qp_rc_[qpind].eos == false));
		if (qp_rc_[qpind].eos == true) {
			return make_pair(Finished, thread_rc_[threadid].output);
		}
		assert(thread_rc_[threadid].recv_comp.status == IBV_WC_SUCCESS);

		int bufind = thread_rc_[threadid].recv_comp.wr_id;
		int bufind1 = bufind/buffperthd_;
		int bufind2 = bufind%buffperthd_;
		recv_buf_[bufind1][bufind2].deserialize();
		//build page with data in recv_buf_[threadid]
		thread_rc_[threadid].inter_buff.buff->resetdata(recv_buf_[bufind1][bufind2].msg, recv_buf_[bufind1][bufind2].datalen);
		//get the source id from received data
		thread_rc_[threadid].inter_buff.recv_node = recv_buf_[bufind1][bufind2].nodeid;
		thread_rc_[threadid].inter_buff.deplete = recv_buf_[bufind1][bufind2].deplete;
		thread_rc_[threadid].inter_buff.bufind = bufind;
		thread_rc_[threadid].inter_buff.curptr = 0;
	}
	assert(true);
}

void DataRecverRcSrOp::threadClose(unsigned short threadid) {
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

	for (int i=0; i<nodenum_; i++) {
		ibv_dereg_mr(qp_rc_[tid2qpid_[threadid]].sendmr[i][threadid]);
	}

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
	numadeallocate(recv_buf_[threadid]);
	numadeallocate(rdma_buf_[threadid]);

	//free buffer
	numadeallocate(thread_rc_[threadid].output);
}

void DataRecverRcSrOp::destroy() {
	TRACE('U');
	char filename[20];
	char nodeID[8];
	snprintf(nodeID, 8, "%d", node_id_);
	strncpy(filename, "rdma_recv_mt_", 20);
	strncat(filename, nodeID, 20);
	DUMP(filename);
}

void DataRecverRcSrOp::RdmaRecv(int bufind, int recv_node, int threadid) {
	//post recv
	struct ibv_recv_wr *bad_wr;
	int qpind = tid2qpid_[threadid];
	qp_rc_[qpind].recv_sge[recv_node][threadid].lkey = qp_rc_[qpind].sendmr[recv_node][bufind/buffperthd_]->lkey;
	qp_rc_[qpind].recv_sge[recv_node][threadid].addr = (uint64_t)recv_buf_[bufind/buffperthd_][bufind%buffperthd_].buffaddr();
	qp_rc_[qpind].recv_wr[recv_node][threadid].wr_id = bufind;
	int ret = ibv_post_recv(qp_rc_[qpind].qp[recv_node], &qp_rc_[qpind].recv_wr[recv_node][threadid], &bad_wr);
	assert(ret == 0);
	atomic_increment(&credit_[recv_node*MAX_THREADS+qpind].credit, 1);

	if ((credit_[recv_node*MAX_THREADS+qpind].credit % credit_writeback_thd_ == 0) || (thread_rc_[threadid].inter_buff.deplete == 1)) {
		RdmaWrite(recv_node, threadid);
	}
}

//thread id is just for tracing
int DataRecverRcSrOp::RdmaWrite(int recv_node, int threadid) {
	int ret = -1;
	int qpind = tid2qpid_[threadid];
	struct ibv_send_wr *bad_wr;
	ret = ibv_post_send(sync_qp_[recv_node], &sync_wr_[recv_node*MAX_THREADS+qpind], &bad_wr);
	assert(ret == 0);
	//poll for completion before send
	ret = ibv_poll_cq(sync_cq_[recv_node], 1025, send_wc_);
	assert(ret >= 0);
	if (ret > 0) {
		assert(send_wc_[0].status == IBV_WC_SUCCESS);
	}

	return ret;
}
