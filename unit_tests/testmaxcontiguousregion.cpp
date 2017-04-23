/*
 * Copyright 2016, Pythia authors (see AUTHORS file).
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

#include "common.h"
#include <vector>
#include <cassert>

using namespace std;

unsigned int findIndexOfMaxContiguousAttribute(const vector<unsigned short>& vec1, 
		const vector<unsigned short>& vec2);

void testOne()
{
	vector<unsigned short> vec1;
	vector<unsigned short> vec2;

	vec1.push_back(3);
	vec1.push_back(4);
	vec1.push_back(5);

	vec2.push_back(0);
	vec2.push_back(1);
	vec2.push_back(5);

	int ret = findIndexOfMaxContiguousAttribute(vec1, vec2);
	assert(ret == 1);
}

void testTwo()
{
	vector<unsigned short> vec1;
	vector<unsigned short> vec2;

	vec1.push_back(3);
	vec1.push_back(4);
	vec1.push_back(5);

	vec2.push_back(0);
	vec2.push_back(1);
	vec2.push_back(2);

	int ret = findIndexOfMaxContiguousAttribute(vec1, vec2);
	assert(ret == 2);
}

void testThree()
{
	vector<unsigned short> vec1;
	vector<unsigned short> vec2;

	vec1.push_back(0);
	vec1.push_back(3);

	vec2.push_back(0);
	vec2.push_back(1);

	int ret = findIndexOfMaxContiguousAttribute(vec1, vec2);
	assert(ret == 0);
}

void testFour()
{
	vector<unsigned short> vec1;
	vector<unsigned short> vec2;

	vec1.push_back(0);

	vec2.push_back(0);

	int ret = findIndexOfMaxContiguousAttribute(vec1, vec2);
	assert(ret == 0);
}

int main()
{
	testOne();
	testTwo();
	testThree();
	testFour();

	return 0;
}
