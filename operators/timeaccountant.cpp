/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
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

#include "../util/numaallocate.h"

/**
 * Decrements \a dest by the current time.
 */
inline
void decrementByCurTime(timespec& dest)
{
	timespec curtime;
	int ret;

	ret = clock_gettime(CLOCK_MONOTONIC_RAW, &curtime);
	assert(ret == 0);

	dest.tv_sec -= curtime.tv_sec;
	dest.tv_nsec -= curtime.tv_nsec;
}

/**
 * Increments \a dest by the current time.
 */
inline
void incrementByCurTime(timespec& dest)
{
	timespec curtime;
	int ret;

	ret = clock_gettime(CLOCK_MONOTONIC_RAW, &curtime);
	assert(ret == 0);

	dest.tv_sec += curtime.tv_sec;
	dest.tv_nsec += curtime.tv_nsec;
}

inline timespec curTime()
{
	timespec curtime;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curtime);
	return curtime;
}

void TimeAccountant::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		times.push_back(NULL);
	}
}

void TimeAccountant::threadInit(unsigned short threadid)
{
	times.at(threadid) = (TimePerOp*) 
		numaallocate_local("TimA", sizeof(TimePerOp), this);
	times.at(threadid)->ScanStartTime.tv_sec = 0;
	times.at(threadid)->ScanStartTime.tv_nsec = 0;
	times.at(threadid)->GetNextTime.tv_sec = 0;
	times.at(threadid)->GetNextTime.tv_nsec = 0;
	times.at(threadid)->ScanStopTime.tv_sec = 0;
	times.at(threadid)->ScanStopTime.tv_nsec = 0;
}

Operator::ResultCode TimeAccountant::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode res;
	timespec& dest = times[threadid]->ScanStartTime;

	decrementByCurTime(dest);
	res = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	incrementByCurTime(dest);

	return res;
}

Operator::GetNextResultT TimeAccountant::getNext(unsigned short threadid)
{
	GetNextResultT res;
	timespec& dest = times[threadid]->GetNextTime;

	decrementByCurTime(dest);
	res = nextOp->getNext(threadid);
	incrementByCurTime(dest);

	return res;
}

Operator::ResultCode TimeAccountant::scanStop(unsigned short threadid)
{
	ResultCode res;
	timespec& dest = times[threadid]->ScanStopTime;

	decrementByCurTime(dest);
	res = nextOp->scanStop(threadid);
	incrementByCurTime(dest);

	return res;
}

void TimeAccountant::threadClose(unsigned short threadid)
{
	if (times.at(threadid) != 0)
	{
		numadeallocate(times.at(threadid));
		times.at(threadid) = 0;
	}
}

/**
 * Returns clock resolution in seconds.
 */
double TimeAccountant::getClockResolution()
{
	double ret;
	timespec resolution;

	ret = clock_getres(CLOCK_MONOTONIC_RAW, &resolution);
	assert(ret == 0);

	ret = resolution.tv_sec + resolution.tv_nsec / 1000.0 / 1000.0 / 1000.0;
	return ret;
}
