
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

//to build Rdma rc connection for data
int DataRecverRcReadOp::RdmaDataChannelConnect(int qpid) {
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

void DataRecverRcReadOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
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

	if (cfg.exists("ibname"))
	{
		ibname_ = (const char*)cfg["ibname"];
	}

	buffperthd_ = buffnum_/threadnum_;

	//need to ensure that every thread has two buffer for each dest
	assert(buffperthd_ >= nodenum_);
	assert(buffperthd_ <= MAX_QP_DEPTH);

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
			thread_rc_[i].deplete[j] = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			for (int k=0; k<MAX_BUFNUM; k++) {
				thread_rc_[i].valid_buf_id[j][k] = -1;
			}
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			if (node_id_ == 0) {
				thread_rc_[i].remoteptr[j] = (buffnum_/threadnum_)%MAX_BUFNUM;
			}
			else {
				thread_rc_[i].remoteptr[j] = 0;
			}
			thread_rc_[i].localptr[j] = 0;
		}
	}
	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_LINKS; j++) {
			thread_rc_[i].deplete[j] = 0;
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		for (int j=0; j<MAX_BUFNUM; j++) {
			thread_rc_[i].free_buf_id[j] = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++) {
		thread_rc_[i].consptr = 0;
		thread_rc_[i].prodptr = 0;
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
}

void DataRecverRcReadOp::threadInit(unsigned short threadid)
{
	//buffer used for rdma recv
	rdma_buf_[threadid] = (RdmaBuf *)numaallocate_local("DRbf", (size_t)msgsize_*buffperthd_, this);
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
			assert(sock_id_[i] != -1);
		}
	}

	assert(rdma_buf_[threadid] != NULL);
	if (threadid == 0) {
		for (int i=0; i<qpnum_; i++) {
			//rdma connect for data channel
			RdmaDataChannelConnect(i);
		}
	}

	pthread_barrier_wait(&barrier_);

	int qpind = tid2qpid_[threadid];
	thread_rc_[threadid].datamr = ibv_reg_mr(qp_rc_[qpind].pd, rdma_buf_[threadid], (size_t)msgsize_*buffperthd_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	assert(thread_rc_[threadid].datamr != NULL);

	//register mem for sync channel
	thread_rc_[threadid].syncmr = ibv_reg_mr(qp_rc_[qpind].pd, (void*)&thread_rc_[threadid].valid_buf_id[0][0], MAX_LINKS*MAX_BUFNUM*sizeof(int), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	assert(thread_rc_[threadid].syncmr != NULL);

	pthread_barrier_wait(&barrier_);
	if  (threadid == 0) {
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
				local_data_qp_info_[i].buf = (uint64_t)&thread_rc_[j].valid_buf_id[i][0];
				local_data_qp_info_[i].rkey = thread_rc_[j].syncmr->rkey;
				local_data_qp_info_[i].size = sizeof(thread_rc_[j].valid_buf_id[i]);
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
	//buffer of data to up operators
	void *space = numaallocate_local("DRpg", sizeof(Page), this);
	thread_rc_[threadid].output = new (space) Page(buffsize, schema.getTupleSize(), this, "DRpg");

	//allocate a Page which can only only one tuple (because it will be checked in buffer.h)
	thread_rc_[threadid].inter_buff.buff = new Page(schema.getTupleSize(), schema.getTupleSize(), this, "DRib");

	for (int i=0; i<buffperthd_; i++) {
		thread_rc_[threadid].free_buf_id[thread_rc_[threadid].prodptr] = threadid*buffperthd_+i;
		thread_rc_[threadid].prodptr++;
		thread_rc_[threadid].prodptr %= MAX_BUFNUM;
	}
	//initialize of wr, so that we can reuse it later
	for (int i=0; i<buffperthd_; i++) {
		memset(&data_wr_[threadid][i], 0, sizeof(data_wr_[threadid][i]));
		data_wr_[threadid][i].send_flags = IBV_SEND_SIGNALED;
		data_wr_[threadid][i].wr_id = threadid*buffperthd_+i;
		data_wr_[threadid][i].next = NULL;
		data_wr_[threadid][i].opcode = IBV_WR_RDMA_READ;
		memset(&data_sge_[threadid][i], 0, sizeof(data_sge_[threadid][i]));
		data_sge_[threadid][i].addr = (uint64_t)recv_buf_[threadid][i].buffaddr();
		data_sge_[threadid][i].length = msgsize_;
		data_sge_[threadid][i].lkey = thread_rc_[threadid].datamr->lkey;
		data_wr_[threadid][i].sg_list = &data_sge_[threadid][i];
		data_wr_[threadid][i].num_sge = 1;
	}

	//initialize of wr, so that we can reuse it later
	for (int i=0; i<MAX_LINKS; i++) {
		memset(&sync_wr_[threadid][i], 0, sizeof(sync_wr_[threadid][i]));
		sync_wr_[threadid][i].send_flags |= IBV_SEND_SIGNALED;
		sync_wr_[threadid][i].send_flags |= IBV_SEND_INLINE;
		sync_wr_[threadid][i].wr_id = buffnum_+1;
		sync_wr_[threadid][i].next = NULL;
		sync_wr_[threadid][i].opcode = IBV_WR_RDMA_WRITE;
		memset(&sync_sge_[threadid][i], 0, sizeof(sync_sge_[threadid][i]));
		sync_sge_[threadid][i].length = sizeof(int);
		sync_sge_[threadid][i].lkey = thread_rc_[threadid].syncmr->lkey;
		sync_wr_[threadid][i].sg_list = &sync_sge_[threadid][i];
		sync_wr_[threadid][i].num_sge = 1;
	}
	if  (threadid == 0) {
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

Operator::ResultCode DataRecverRcReadOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode = Operator::Ready;
	return rescode;
}

Operator::GetNextResultT DataRecverRcReadOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
	//poll for recv completion
	int qpind = tid2qpid_[threadid];
	void *tuple;
	thread_rc_[threadid].output->clear();
	while (1) {
		while ((tuple = (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr))) {
			//output large enough to host all data in inter_buff
			size_t leftdata_in_interbuff = thread_rc_[threadid].inter_buff.leftdata();
			if (thread_rc_[threadid].output->canStore(leftdata_in_interbuff)) {
				void *bucketspace = thread_rc_[threadid].output->allocate(leftdata_in_interbuff);
				memcpy(bucketspace, (thread_rc_[threadid].inter_buff.buff)->getTupleOffset(thread_rc_[threadid].inter_buff.curptr), leftdata_in_interbuff);
				//used up data in inter_buff, release the buffer
				ReleaseBuffer(thread_rc_[threadid].inter_buff.recv_node, thread_rc_[threadid].inter_buff.dest_bufid, threadid);
				//return free buffer to the original thread
				int origtid = thread_rc_[threadid].inter_buff.bufind/buffperthd_;
				int pos = atomic_module_increase(&thread_rc_[origtid].prodptr, 1, MAX_BUFNUM);
				thread_rc_[origtid].free_buf_id[pos] = thread_rc_[threadid].inter_buff.bufind;
				break;
			}
			//output not large enough to host all data in inter_buff
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
		volatile int recvcomp = 0;
		//post as many read request as possible
		do {
			AttemptToPostRead(threadid);
		} while (((recvcomp = PollDataChannelCQ(threadid)) == 0) && (qp_rc_[qpind].eos == false));

		if (qp_rc_[qpind].eos == true) {
			return make_pair(Finished, thread_rc_[threadid].output);
		}

		assert(thread_rc_[threadid].recv_comp.status == IBV_WC_SUCCESS);
		int bufind = thread_rc_[threadid].recv_comp.wr_id;
		int bufind1 = bufind/buffperthd_;
		int bufind2 = bufind%buffperthd_;
		recv_buf_[bufind1][bufind2].deserialize();
		//build page with data in rdma_buf_
		thread_rc_[threadid].inter_buff.buff->resetdata(recv_buf_[bufind1][bufind2].msg, recv_buf_[bufind1][bufind2].datalen);
		//clear the curptr to 0
		thread_rc_[threadid].inter_buff.clear();
		//get the source id from received data
		thread_rc_[threadid].inter_buff.recv_node = recv_buf_[bufind1][bufind2].nodeid;
		thread_rc_[threadid].inter_buff.deplete = recv_buf_[bufind1][bufind2].deplete;
		thread_rc_[threadid].inter_buff.bufind = bufind;
		thread_rc_[threadid].inter_buff.dest_bufid = recv_buf_[bufind1][bufind2].dest_bufid;
		thread_rc_[threadid].inter_buff.curptr = 0;
	}
	assert(true);
}

void DataRecverRcReadOp::threadClose(unsigned short threadid) {
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
	numadeallocate(recv_buf_[threadid]);
}

void DataRecverRcReadOp::destroy() {
	TRACE('U');
	char filename[20];
	char nodeID[8];
	snprintf(nodeID, 8, "%d", node_id_);
	strncpy(filename, "rdma_recv_mt_", 20);
	strncat(filename, nodeID, 20);
	DUMP(filename);
}

int DataRecverRcReadOp::PollDataChannelCQ(int threadid) {
	int comp = 0;
	int qpind = tid2qpid_[threadid];
	do {
		thread_rc_[threadid].recv_comp.wr_id = 0;
		comp = ibv_poll_cq(qp_rc_[qpind].cq, 1, &thread_rc_[threadid].recv_comp);
		if (comp != 0) {
			assert(thread_rc_[threadid].recv_comp.status == IBV_WC_SUCCESS);
		}
	} while (thread_rc_[threadid].recv_comp.wr_id > buffnum_); //distinguish the comp of sync and data, since we're using the same channel
	return comp;
}

int DataRecverRcReadOp::AttemptToPostRead(int threadid) {
	int dest_id, bufid, localbufid, localfullbufid;
	int qpind = tid2qpid_[threadid];
	if (thread_rc_[threadid].free_buf_id[thread_rc_[threadid].consptr] == -1) {
		return 0;
	}
	if (FindValidBuf(&dest_id, &bufid, threadid) == -1) {
		return 0;
	}
	localfullbufid = thread_rc_[threadid].free_buf_id[thread_rc_[threadid].consptr];
	localbufid = localfullbufid%buffperthd_;
	assert(localfullbufid/buffperthd_ == threadid);

	thread_rc_[threadid].free_buf_id[thread_rc_[threadid].consptr] = -1;
	thread_rc_[threadid].consptr++;
	thread_rc_[threadid].consptr %= MAX_BUFNUM;

	data_wr_[threadid][localbufid].wr.rdma.remote_addr = (uint64_t)thread_rc_[threadid].data_qpinfo[dest_id].buf+bufid*msgsize_;
	data_wr_[threadid][localbufid].wr.rdma.rkey = thread_rc_[threadid].data_qpinfo[dest_id].rkey;
	struct ibv_send_wr *bad_wr;
	assert(data_wr_[threadid][localbufid].opcode == IBV_WR_RDMA_READ);
	assert(ibv_post_send(qp_rc_[qpind].qp[dest_id], &data_wr_[threadid][localbufid], &bad_wr) == 0);
	return 0;
}

int DataRecverRcReadOp::FindValidBuf(int* nodeid, int* bufid, int threadid) {
	for (int i=0; i<nodenum_; i++) {
		if (thread_rc_[threadid].valid_buf_id[i][thread_rc_[threadid].localptr[i]] != -1) {
			*bufid = thread_rc_[threadid].valid_buf_id[i][thread_rc_[threadid].localptr[i]];
			thread_rc_[threadid].valid_buf_id[i][thread_rc_[threadid].localptr[i]] = -1;
			thread_rc_[threadid].localptr[i]++;
			thread_rc_[threadid].localptr[i] %= MAX_BUFNUM;
			*nodeid = i;
			return 0;
		}
	}
	return -1;
}

//todo: release more than one buffer at one function call
int DataRecverRcReadOp::ReleaseBuffer(int dest_id, int bufid, int threadid) {
	int qpind = tid2qpid_[threadid];
	int origtid = bufid/buffperthd_;
	int origbufid = bufid%buffperthd_;
	sync_sge_[threadid][dest_id].addr = (uint64_t)&origbufid;
	sync_wr_[threadid][dest_id].wr.rdma.rkey = thread_rc_[origtid].sync_qpinfo[dest_id].rkey;
	int pos = atomic_module_increase(&thread_rc_[origtid].remoteptr[dest_id], 1, MAX_BUFNUM);
	sync_wr_[threadid][dest_id].wr.rdma.remote_addr = thread_rc_[origtid].sync_qpinfo[dest_id].buf + pos*sizeof(int);

	struct ibv_send_wr *bad_wr;
	assert(sync_wr_[threadid][dest_id].opcode == IBV_WR_RDMA_WRITE);
	assert(ibv_post_send(qp_rc_[qpind].qp[dest_id], &sync_wr_[threadid][dest_id], &bad_wr) == 0);
	return 0;
}

inline int DataRecverRcReadOp::atomic_module_increase(volatile int* ptr, int by,int module) {
	int ret, oldval;
	do {
		oldval = *ptr;
		ret = atomic_compare_and_swap(ptr, oldval, (oldval+by)%module);
	} while (oldval != ret);
	return ret;
}

