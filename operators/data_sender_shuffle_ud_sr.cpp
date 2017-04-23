
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
static unsigned long TraceLogTail[TraceMaxThreads] =
{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	//  << 16 zeros
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
int DataSenderShuffleUdSrOp::RdmaConnect(int qpind)
{
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
	qp_rc_[qpind].cq = ibv_create_cq(qp_rc_[qpind].ctx, buffnum_/qpnum_*2+1, NULL, NULL, 0);
	qp_rc_[qpind].pd = ibv_alloc_pd(qp_rc_[qpind].ctx);
	struct ibv_qp_init_attr qp_init_attr;
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.send_cq = qp_rc_[qpind].cq;
	qp_init_attr.recv_cq = qp_rc_[qpind].cq;
	qp_init_attr.cap.max_send_wr  = buffnum_/qpnum_;
	qp_init_attr.cap.max_recv_wr  = 1;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.cap.max_inline_data = 0;
	qp_init_attr.qp_type = IBV_QPT_UD;

	qp_rc_[qpind].qp = ibv_create_qp(qp_rc_[qpind].pd, &qp_init_attr);
	struct ibv_qp_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state   = IBV_QPS_INIT;
	qp_attr.pkey_index   = 0;
	qp_attr.port_num   = 1;
	qp_attr.qkey       = 0x11111111;

	ret = ibv_modify_qp(qp_rc_[qpind].qp, &qp_attr,
	                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY);
	struct ibv_port_attr ptattr;
	ibv_query_port(qp_rc_[qpind].ctx, 1, &ptattr);

	udinfo.lid = ptattr.lid;
	udinfo.qpn = qp_rc_[qpind].qp->qp_num;
	udinfo.psn = rand() & 0xffffff;

	for (int i=0; i<nodenum_; i++)
	{
		assert(sock_id_[i] != -1);
		recv(sock_id_[i], &qp_rc_[qpind].remote_qpinfo[i], sizeof(struct qpinfo), MSG_WAITALL);
	}

	for (int i=0; i<nodenum_; i++)
	{
		assert(sock_id_[i] != -1);
		struct ibv_ah_attr ah_attr;
		memset(&ah_attr, 0, sizeof(ah_attr));
		ah_attr.is_global    = 0;
		ah_attr.dlid     = qp_rc_[qpind].remote_qpinfo[i].lid;
		ah_attr.sl       = 0;
		ah_attr.src_path_bits  = 0;
		ah_attr.port_num   = 1;

		struct ibv_ah *ah = ibv_create_ah(qp_rc_[qpind].pd, &ah_attr);
		qp_rc_[qpind].rdma_ah[i] = ah;
	}

	struct ibv_qp_attr dgram_attr;
	memset(&dgram_attr, 0, sizeof(dgram_attr));
	dgram_attr.qp_state     = IBV_QPS_RTR;

	ret = ibv_modify_qp(qp_rc_[qpind].qp, &dgram_attr, IBV_QP_STATE);

	memset(&dgram_attr, 0, sizeof(dgram_attr));
	dgram_attr.qp_state   = IBV_QPS_RTS;
	dgram_attr.sq_psn   = udinfo.psn;

	ret = ibv_modify_qp(qp_rc_[qpind].qp, &dgram_attr, IBV_QP_STATE|IBV_QP_SQ_PSN);

	return ret;
}

//to build Rdma uc connection for sync
void DataSenderShuffleUdSrOp::RdmaWriteConnect(credit_t *credit_array)
{
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
	for (unsigned int i = 0; i<sock_id_.size(); i++)
	{
		sync_pd_[i] = ibv_alloc_pd(sync_ctx_[i]);
		sync_cq_[i] = ibv_create_cq(sync_ctx_[i], MAX_BUFF_NUM+1, NULL, NULL, 0);

		struct ibv_qp_init_attr qp_init_attr;
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.send_cq = sync_cq_[i];
		qp_init_attr.recv_cq = sync_cq_[i];
		qp_init_attr.cap.max_send_wr  = 1;
		qp_init_attr.cap.max_recv_wr  = MAX_BUFF_NUM;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		qp_init_attr.cap.max_inline_data = 64;
		qp_init_attr.qp_type = IBV_QPT_UC;

		sync_qp_[i] = ibv_create_qp(sync_pd_[i], &qp_init_attr);
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state   = IBV_QPS_INIT;
		qp_attr.pkey_index   = 0;
		qp_attr.port_num   = 1;
		qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

		ibv_modify_qp(sync_qp_[i], &qp_attr,
		                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
		sync_sendmr_[i] = ibv_reg_mr(sync_pd_[i], (void*)&credit_array[i*MAX_THREADS],
		                             sizeof(credit_array[i])*MAX_THREADS,
		                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
		struct ibv_port_attr ptattr;
		ibv_query_port(sync_ctx_[i], 1, &ptattr);

		rdma_write_ucinfo_[i].lid = ptattr.lid;
		rdma_write_ucinfo_[i].qpn = sync_qp_[i]->qp_num;
		rdma_write_ucinfo_[i].psn = rand() & 0xffffff;
		rdma_write_ucinfo_[i].buf = (uint64_t)&credit_array[i*MAX_THREADS];
		rdma_write_ucinfo_[i].rkey = sync_sendmr_[i]->rkey;
		rdma_write_ucinfo_[i].size = sizeof(credit_array[i])*MAX_THREADS;
	}

	for (unsigned int i=0; i<sock_id_.size(); i++)
	{
		if (sock_id_[i] != -1)
		{
			send(sock_id_[i], &rdma_write_ucinfo_[i], sizeof(rdma_write_ucinfo_[i]), MSG_DONTWAIT);
		}
	}

	for (unsigned int i=0; i<sock_id_.size(); i++)
	{
		if (sock_id_[i] != -1)
		{
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

void DataSenderShuffleUdSrOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	static_assert(sizeof(struct thread_rc_t)%CACHE_LINE_SIZE == 0);
	static_assert(sizeof(struct qp_rc_t)%CACHE_LINE_SIZE == 0);
	static_assert(sizeof(struct RdmaBuf) == MSG_SIZE);

	Operator::init(root, cfg);
	schema = nextOp->getOutSchema();
	//handle partition attribute and hashing stuff
	hashfn = TupleHasher::create(schema, cfg["hash"]);
	msgsize_ = cfg["msgsize"];
	buffnum_ = cfg["buffnum"];
	if (cfg.exists("threadnum"))
	{
		threadnum_ = cfg["threadnum"];
	}
	else
	{
		threadnum_ = 1;
	}
	if (cfg.exists("qpnum"))
	{
		qpnum_ = cfg["qpnum"];
	}
	else
	{
		qpnum_ = 1;
	}
	if (cfg.exists("pollcqnum"))
	{
		pollcqnum_ = cfg["pollcqnum"];
	}
	else
	{
		pollcqnum_ = 1;
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
	assert(buffperthd_ <= MAX_QP_DEPTH);
	assert(buffperthd_ >= 1);
	nodenum_ = cfg["nodenum"];
	assert(nodenum_ < MAX_LINKS);

	node_id_ = cfg["nodeid"]; //node id is to identify each node, frange from 0 to n-1
	host_ip_ = (const char*) cfg["hostIP"];
	//loop to get all ip adress of remote node
	libconfig::Setting& ipgrp = cfg["destIP"];
	int size = ipgrp.getLength();

	assert( size != 0 );
	assert( size <= MAX_LINKS );

	for (int i=0; i<size; ++i)
	{
		std::string ipaddr = (const char*) ipgrp[i];
		dest_ip_.push_back(ipaddr);
	}
	for (int i=0; i<size; i++)
	{
		sock_id_.push_back(-1);
	}

	//buffer for data to each destination
	for (int i=0; i<MAX_THREADS; i++)
	{
		for (int j=0; j<MAX_LINKS; j++)
		{
			thread_rc_[i].out_buffer[j] = &EmptyPage;
		}
	}
	for (int i=0; i<MAX_THREADS; i++)
	{
		for (int j=0; j<MAX_LINKS; j++)
		{
			thread_rc_[i].out_buffer_id[j] = -1;
		}
	}
	for (int i=0; i<MAX_LINKS; i++)
	{
		for (int j=0; j<MAX_THREADS; j++)
		{
			credit_[i*MAX_THREADS+j].credit = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++)
	{
		thread_rc_[i].initial_buf_cnt = 0;
		for (int j=0; j<MAX_BUFF_NUM; j++)
		{
			thread_rc_[i].initial_buf_id[j] = -1;
		}
	}

	for (int i=0; i<MAX_THREADS; i++)
	{
		for (int j=0; j<MAX_LINKS; j++)
		{
			qp_rc_[i].local_credit[j].credit = 0;
		}
	}

	for (int i=0; i<MAX_THREADS; i++)
	{
		for (int j=0; j<MAX_LINKS; j++)
		{
			qp_rc_[i].pd = NULL;
		}
	}

	for (int i=0; i<MAX_THREADS; i++)
	{
		thread_rc_[i].consptr = 0;
	}
	//populate rdma structs for sync channel
	//one channel for each destination
	for (int i=0; i<size; i++)
	{
		sync_cq_.push_back(NULL);
	}
	for (int i=0; i<size; i++)
	{
		sync_qp_.push_back(NULL);
	}
	for (int i=0; i<size; i++)
	{
		sync_sendmr_.push_back(NULL);
	}
	for (int i=0; i<size; i++)
	{
		sync_ctx_.push_back(NULL);
	}
	for (int i=0; i<size; i++)
	{
		sync_pd_.push_back(NULL);
	}

	for (int i=0; i<threadnum_; i++)
	{
		tid2qpid_[i] = i*qpnum_/threadnum_;
	}

	pthread_barrier_init(&barrier_, NULL, threadnum_);
#ifdef TRACESENDLOG
	TraceLog = (TraceEntry*)numaallocate_local("TRLG", sizeof(TraceEntry)*TraceLogSize*TraceMaxThreads,
	           this);
#endif
}

void DataSenderShuffleUdSrOp::threadInit(unsigned short threadid)
{
	int qpind = tid2qpid_[threadid];
	assert(threadid < threadnum_);
	//build connection here
	if (threadid == 0)
	{
		//TcpServer(12345, host_ip_.c_str());
		TcpServer(LISTEN_PORT+operator_id_, host_ip_.c_str(), nodenum_, sock_id_);

		for (int i=0; i<nodenum_; i++)
		{
			assert(sock_id_[i] != -1);
		}
	}
	for (int i=0; i<buffperthd_; i++)
	{
		thread_rc_[threadid].initial_buf_id[i] = threadid*buffperthd_+i;
		thread_rc_[threadid].initial_buf_cnt++;
	}

	//create buffers for rdma
	send_buf_[threadid] = (RdmaBuf *)numaallocate_local("DSbf", sizeof(RdmaBuf)*buffperthd_, this);
	memset(send_buf_[threadid], 0, sizeof(RdmaBuf)*buffperthd_);

	if (threadid == 0)
	{
		for (int i=0; i<qpnum_; i++)
		{
			RdmaConnect(i);
		}
	}

	pthread_barrier_wait(&barrier_);

	assert(qp_rc_[qpind].pd != NULL);
	thread_rc_[threadid].sendmr = ibv_reg_mr(qp_rc_[qpind].pd, send_buf_[threadid],
	                              sizeof(RdmaBuf)*buffperthd_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
	                              IBV_ACCESS_REMOTE_WRITE);

	//populate send_sge_, put it here because it needs the value of local mr
	for (int i=0; i<nodenum_; i++)
	{
		memset(&thread_rc_[threadid].send_sge[i], 0, sizeof(struct ibv_sge));
		thread_rc_[threadid].send_sge[i].length = sizeof(struct RdmaBuf);
	}

	for (int i=0; i<nodenum_; i++)
	{
		memset(&thread_rc_[threadid].send_wr[i], 0, sizeof(struct ibv_send_wr));
		thread_rc_[threadid].send_wr[i].wr.ud.ah = qp_rc_[qpind].rdma_ah[i];
		thread_rc_[threadid].send_wr[i].wr.ud.remote_qpn = qp_rc_[qpind].remote_qpinfo[i].qpn;
		thread_rc_[threadid].send_wr[i].wr.ud.remote_qkey = 0x11111111;
		thread_rc_[threadid].send_wr[i].send_flags = IBV_SEND_SIGNALED;
		thread_rc_[threadid].send_wr[i].next = NULL;
		thread_rc_[threadid].send_wr[i].opcode = IBV_WR_SEND;
		thread_rc_[threadid].send_wr[i].sg_list = &thread_rc_[threadid].send_sge[i];
		thread_rc_[threadid].send_wr[i].num_sge = 1;
	}

	pthread_barrier_wait(&barrier_);

	if (threadid == 0)
	{
		RdmaWriteConnect(credit_);
	}

	if (threadid == 0)
	{
		//sync to exit
		for (unsigned int i=0; i<sock_id_.size(); i++)
		{
			if(sock_id_[i] == -1)
			{
				continue;
			}
			char temp = 'a';
			assert(send(sock_id_[i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
		}

		for (unsigned int i=0; i<sock_id_.size(); i++)
		{
			if(sock_id_[i] == -1)
			{
				continue;
			}
			char temp = 'b';
			assert(recv(sock_id_[i], &temp, sizeof(temp), MSG_WAITALL) != -1);
		}
	}
}

Operator::ResultCode DataSenderShuffleUdSrOp::scanStart(unsigned short threadid,
        Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode rescode;

	for (int i=0; i<nodenum_; i++)
	{
		int bufid;
		thread_rc_[threadid].initial_buf_cnt--;
		assert(thread_rc_[threadid].initial_buf_cnt >= 0);
		bufid = thread_rc_[threadid].initial_buf_id[thread_rc_[threadid].initial_buf_cnt];
		//new a page once, then call resetandclear() afterwards
		thread_rc_[threadid].out_buffer[i] = new Page(send_buf_[bufid/buffperthd_][bufid%buffperthd_].msg,
		        sizeof(send_buf_[bufid/buffperthd_][bufid%buffperthd_].msg), NULL, schema.getTupleSize());
		thread_rc_[threadid].out_buffer[i]->clear();
		thread_rc_[threadid].out_buffer_id[i] = bufid;
	}

	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	return rescode;
}

Operator::GetNextResultT DataSenderShuffleUdSrOp::getNext(unsigned short threadid)
{
	//here we assume a one to one mapping between rdma wr_id and send buff
	//addr and 0 to buffer0, 1 to buffer1, buffnum_-1 to buffer buffnum_-1
	int qpind = tid2qpid_[threadid];
	Operator::GetNextResultT result;
	result = nextOp->getNext(threadid);
	Page* in;
	Operator::ResultCode rc;
	in = result.second;
	rc = result.first;

	void *tuple;
	int tupoffset = 0;
	unsigned int hashbucket;
	while (1)
	{
		while ((tuple = in->getTupleOffset(tupoffset++)) != NULL)
		{
			//hash to decide destination
			hashbucket = hashfn.hash(tuple);
			void * bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();

			//now full, already serialized
			if (bucketspace == NULL)
			{
				RdmaSend(threadid, thread_rc_[threadid].out_buffer_id[hashbucket], hashbucket, MoreData);
				//now find new buffers
				int bufid = -1;
				if (thread_rc_[threadid].initial_buf_cnt >= 1)
				{
					thread_rc_[threadid].initial_buf_cnt--;
					bufid = thread_rc_[threadid].initial_buf_id[thread_rc_[threadid].initial_buf_cnt];
				}
				else
				{
					int comp = -1;
					//currently only work for MESQ mode, not considering contention for simplicity
					if (thread_rc_[threadid].consptr > 0)
					{
						thread_rc_[threadid].consptr--;
						bufid = thread_rc_[threadid].free_buf_id[thread_rc_[threadid].consptr];
					}
					else
					{
						do
						{
							comp = ibv_poll_cq(qp_rc_[qpind].cq, pollcqnum_, thread_rc_[threadid].send_wc);
							assert(comp != -1);
							if (comp > 0)
							{
								for (int i=0; i<(comp-1); i++)
								{
									thread_rc_[threadid].free_buf_id[thread_rc_[threadid].consptr++] =
									    thread_rc_[threadid].send_wc[i].wr_id;
								}
								bufid = thread_rc_[threadid].send_wc[comp-1].wr_id;
							}
						}
						while (bufid == -1);
					}
				}
				assert(bufid != -1);

				thread_rc_[threadid].out_buffer[hashbucket]->resetandclear(
				    send_buf_[bufid/buffperthd_][bufid%buffperthd_].msg,
				    sizeof(send_buf_[bufid/buffperthd_][bufid%buffperthd_].msg));
				//to record current bufid used by this out buffer
				thread_rc_[threadid].out_buffer_id[hashbucket] = bufid;
				bucketspace = thread_rc_[threadid].out_buffer[hashbucket]->allocateTuple();
			}
			dbgassert(bucketspace != NULL);
			schema.copyTuple(bucketspace, tuple);
		}
		if (rc == Finished)
		{
			for (int i=0; i<nodenum_; i++)
			{
				RdmaSend(threadid, thread_rc_[threadid].out_buffer_id[i], i, Depleted);
			}
			return make_pair(Finished, &EmptyPage);
		}

		result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}
}

void DataSenderShuffleUdSrOp::threadClose(unsigned short threadid)
{
	//shake hands to exit
	//block until all destinations receive all the data
	if (threadid == 0)
	{
		for (unsigned int i=0; i<sock_id_.size(); i++)
		{
			if(sock_id_[i] == -1)
			{
				continue;
			}
			char temp = 'a';
			assert(send(sock_id_[i], &temp, sizeof(temp), MSG_DONTWAIT) != -1);
		}

		for (unsigned int i=0; i<sock_id_.size(); i++)
		{
			if(sock_id_[i] == -1)
			{
				continue;
			}
			char temp = 'b';
			assert(recv(sock_id_[i], &temp, sizeof(temp), MSG_WAITALL) != -1);
		}

		//close sockets
		for (unsigned int i=0; i<sock_id_.size(); i++)
		{
			close(sock_id_[i]);
		}
	}

	pthread_barrier_wait(&barrier_);
	ibv_dereg_mr(thread_rc_[threadid].sendmr);

	if (threadid == 0)
	{
		//close rdma channel
		for (int i=0; i<qpnum_; i++)
		{
			for (int j=0; j<nodenum_; j++)
			{
				ibv_destroy_ah(qp_rc_[i].rdma_ah[j]);
			}
		}
		for (int i=0; i<qpnum_; i++)
		{
			ibv_destroy_qp(qp_rc_[i].qp);
		}
		for (int i=0; i<qpnum_; i++)
		{
			ibv_dealloc_pd(qp_rc_[i].pd);
		}
		for (int i=0; i<qpnum_; i++)
		{
			ibv_destroy_cq(qp_rc_[i].cq);
		}
		for (int i=0; i<qpnum_; i++)
		{
			ibv_close_device(qp_rc_[i].ctx);
		}
	}

	numadeallocate(send_buf_[threadid]);
	//delete Page in out_buffer_
	for (int i=0; i<MAX_LINKS; i++)
	{
		if (thread_rc_[threadid].out_buffer[i] != &EmptyPage)
		{
			delete thread_rc_[threadid].out_buffer[i];
		}
	}

}

void DataSenderShuffleUdSrOp::destroy()
{
	TRACE('U');
	char filename[20];
	char nodeID[8];
	snprintf(nodeID, 8, "%d", node_id_);
	strncpy(filename, "rdma_send_mt_", 20);
	strncat(filename, nodeID, 20);
	DUMP(filename);
}

int DataSenderShuffleUdSrOp::RdmaSend(int threadid, int bufid, unsigned int dest_id, deplete_t deplete)
{
	int ret = 0;
	assert(bufid != -1);
	Page *nsendPage = thread_rc_[threadid].out_buffer[dest_id];

	int sendbuf_ind1 = bufid/buffperthd_;
	int sendbuf_ind2 = bufid%buffperthd_;

	int buflen = nsendPage->getUsedSpace();
	send_buf_[sendbuf_ind1][sendbuf_ind2].datalen = buflen;
	send_buf_[sendbuf_ind1][sendbuf_ind2].nodeid = node_id_;
	//this should be set in RdmaSend, we want to make sure that the deplete data
	//is sent as the last packet
	//send_buf_[threadid][bufid].deplete = MoreData;
	send_buf_[sendbuf_ind1][sendbuf_ind2].deplete = deplete;

	while (attemptToGetCredit(threadid, dest_id) == false) {};

	PostSend(dest_id, bufid, threadid);
	ret++;
	return ret;
}

//attemp to get credit, return true if succeed
inline bool DataSenderShuffleUdSrOp::attemptToGetCredit(int threadid, int dest_id)
{
	int qpind = tid2qpid_[threadid];
	int newcredit = qp_rc_[qpind].local_credit[dest_id].credit;
	int oldcredit;
	do
	{
		if (credit_[dest_id*MAX_THREADS+qpind].credit <= qp_rc_[qpind].local_credit[dest_id].credit)
		{
			return false;
		}
		oldcredit = newcredit;
		newcredit = oldcredit + 1;
		newcredit = atomic_compare_and_swap(&qp_rc_[qpind].local_credit[dest_id].credit, oldcredit,
		                                    newcredit);
	}
	while (oldcredit != newcredit);

	return true;
}

//inline function for post rdma send
inline int DataSenderShuffleUdSrOp::PostSend(int dest_id, int bufid, int threadid)
{
	int qpind = tid2qpid_[threadid];
	thread_rc_[threadid].send_sge[dest_id].addr = (uint64_t)
	        &send_buf_[bufid/buffperthd_][bufid%buffperthd_];
	thread_rc_[threadid].send_sge[dest_id].lkey = thread_rc_[bufid/buffperthd_].sendmr->lkey;
	thread_rc_[threadid].send_wr[dest_id].wr_id = bufid;
	struct ibv_send_wr *bad_wr;

	int ret = 0;
	ret = ibv_post_send(qp_rc_[qpind].qp, &thread_rc_[threadid].send_wr[dest_id], &bad_wr);
	assert(ret == 0);
	return ret;
}
