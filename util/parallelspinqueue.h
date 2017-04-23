
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

#include "custom_asserts.h"
#include "../lock.h"

template <typename T, int size>
class ParallelSpinQueue
{
	public:
		ParallelSpinQueue()
			: prodpointer(0), conspointer(0), rundown(false)
		{
		}

		~ParallelSpinQueue()
		{
		}

		enum ResultT
		{
			Okay,
			Rundown,
			Empty,
			Full
		};

		/**
		 * Pushes what is pointed to by \a datain in the queue. 
		 * @return \a Okay, if item was stored in the queue. 
		 * @return \a Rundown, if queue is in rundown.
		 * @return \a Full, if queue is full and not in rundown.
		 * @return Call will never return \a Empty.
		 */
		ResultT push(T* datain);

		/**
		 * Pops one item out of the queue, and writes it to the space pointed
		 * to by \a dataout. 
		 * @return \a Okay, if an item was returned.
		 * @return \a Empty, if queue is empty and not in rundown.
		 * @return \a Rundown, if the queue is empty and in rundown. 
		 * @return Call will never return \a Full.
		 */
		ResultT pop(T* dataout);

		/**
		 * Sets queue in rundown mode, and signals all waiters that the queue
		 * is in rundown. All pending and future push/pop operations will
		 * eventually return Rundown and fail.
		 */
		void signalRundown();

		/**
		 * Checks if empty *without acquiring lock*.
		 * Return is not to be trusted.
		 */
		inline bool maybeEmpty()
		{
			return (isEmpty() == true);
		}

	private:
		Lock lock;

		T queue[size];

		volatile int prodpointer; //< Slot that next push will write in.
		volatile int conspointer; //< Slot that next pop will return.
		volatile bool rundown;

		inline bool isFull()
		{
			return (prodpointer == ((conspointer + size - 1) % size));
		}

		inline bool isEmpty()
		{
			return (prodpointer == conspointer);
		}

		inline void incrementProd()
		{
			prodpointer = (prodpointer + 1) % size;
		}

		inline void incrementCons()
		{
			conspointer = (conspointer + 1) % size;
		}
};

#include "parallelspinqueue.inl"
