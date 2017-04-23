
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

template <typename T, int size>
typename ParallelSpinQueue<T, size>::ResultT  ParallelSpinQueue<T, size>::push(T* datain)
{
	typename ParallelSpinQueue<T, size>::ResultT ret = Okay;

	if (rundown)
	{
		ret = Rundown;
		return ret;
	}

	lock.lock();

	if (isFull())
	{
		ret = Full;
	}
	else
	{
		// Queue is not in rundown or full. Write item.
		//
		queue[prodpointer] = *datain;
		incrementProd();
	}

	lock.unlock();
	return ret;
}

template <typename T, int size>
typename ParallelSpinQueue<T, size>::ResultT  ParallelSpinQueue<T, size>::pop(T* dataout)
{
	typename ParallelSpinQueue<T, size>::ResultT ret = Okay;

	lock.lock();

	if (isEmpty())
	{
		if (rundown)
		{
			ret = Rundown;
		}
		else
		{
			ret = Empty;
		}
	}
	else
	{
		// Queue is not empty (but might be in rundown). Return item.
		//
		*dataout = queue[conspointer];
		incrementCons();
	}

	lock.unlock();

	return ret;
}

template <typename T, int size>
void ParallelSpinQueue<T, size>::signalRundown()
{
	rundown = true;
}
