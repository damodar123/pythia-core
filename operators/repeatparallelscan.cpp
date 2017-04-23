
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

using std::make_pair;

static unsigned short INVALIDENTRY = static_cast<unsigned short>(-1);

void RepeatParallelScanOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ParallelScanOp::init(root, cfg);

	// Number of repetitions.
	//
	int temprepeatnum = cfg["repeat"];
	repeatnum = (unsigned short) temprepeatnum;

	// Populate metadata.
	//
	for (unsigned int i = 0; i < vec_tbl.size(); ++i)
	{
		RepeatParallelScanMetadataT temp = RepeatParallelScanMetadataT();
		temp.repeat = repeatnum;
		temp.lock.reset();
		vec_rpsm.push_back(temp);
	}
}

Operator::GetNextResultT RepeatParallelScanOp::getNext(unsigned short threadid)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];
	dbgassert(vec_tbl.at(groupno) != NULL);

	vec_rpsm[groupno].lock.lock();
	TupleBuffer* ret = vec_tbl[groupno]->readNext();
	while ((ret == NULL) && (vec_rpsm[groupno].repeat > 1))
	{
		--vec_rpsm[groupno].repeat;
		vec_tbl[groupno]->reset();
		ret = vec_tbl[groupno]->readNext();
	}
	vec_rpsm[groupno].lock.unlock();

	if (ret == NULL) {
		return make_pair(Operator::Finished, &EmptyPage);
	} else {
		return make_pair(Operator::Ready, ret);
	}
}
