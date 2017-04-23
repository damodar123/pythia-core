
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

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <arpa/inet.h>

#include <vector>
#include <cassert>
#include <stdlib.h>
#include <unistd.h>
#include <cstdio>
#include <string>
#include <string.h>
#include <iostream>

using namespace std;

int TcpServer(int port, const char *host, int nodenum, vector<int>& conn_sock)
{
	//set up time between bind attempt
	const int timeout = 20;
	//set up tcp ip socket, transferring rdma info
	int sock_fd, conn_fd = 0;
	int opt = 1;
	struct sockaddr_in addr_serv, addr_client;

	sock_fd = socket(AF_INET, SOCK_STREAM, 0);

	//make the port reusable
	int setsocktval;
	setsocktval = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	assert(setsocktval == 0);

	memset(&addr_serv, 0, sizeof(addr_serv));
	addr_serv.sin_family = AF_INET;
	addr_serv.sin_port = htons(port);
	if (inet_aton(host, &addr_serv.sin_addr) == 0)
	{
		//use printf rather than cout because the cout will be redirected to a temp
		//file when running multiple queries, and sometimes the temp file will not
		//be copied out if the query fails, while the output of printf will always
		//be shown in the stdout where we can find in the pbs job output file
		std::cerr << "sock address error" << std::endl;
		return -1;
	}

	int client_len = sizeof(struct sockaddr_in);
	int i, clientid;
	const int retries = 5;
	int trial = 0;
	while ((trial < retries) && (bind(sock_fd, (struct sockaddr *)&addr_serv, sizeof(struct sockaddr_in)) < 0))
	{
		sleep(timeout);
		++trial;
	}

	if (trial == retries) {
		//timeout handling code
		std::cerr << "Feilong TcpServer bind error" << std::endl;
		return -1;
	}

	if (listen(sock_fd, nodenum) < 0)
	{
		//to print out error information
		std::cerr << "Feilong listen fail" << std::endl;
		return -1;
	}

	fd_set readfds;
	FD_ZERO(&readfds);
	FD_SET(sock_fd, &readfds);
	int maxfds = sock_fd;

	for (i = 0; i < nodenum; i++)
	{
		int selectval;
		selectval = select(maxfds+1, &readfds, NULL, NULL, NULL);
		assert(selectval >= 0);

		if (FD_ISSET(sock_fd, &readfds))
		{
			conn_fd = ::accept(sock_fd, (struct sockaddr *)&addr_client, (socklen_t*)&client_len);
			assert(conn_fd > 0);
			recv(conn_fd, &clientid, sizeof(clientid), MSG_WAITALL);
			conn_sock[clientid] = conn_fd;
		}
	}
	close(sock_fd);

	return 0;
}

int TcpClient(int port, const vector<string>& dest_ip, int nodenum, vector<int>& conn_sock, int node_id)
{
	vector<int> sock_fd (nodenum, -1);
	vector<struct sockaddr_in> addr_serv;
	addr_serv.resize(nodenum);

	for (int i = 0; i < nodenum; i++)
	{
		sock_fd[i] = socket(AF_INET, SOCK_STREAM, 0);
	}

	for (int i = 0; i < nodenum; i++)
	{
		memset(&addr_serv[i], 0, sizeof(addr_serv[i]));
		addr_serv[i].sin_family = AF_INET;
		addr_serv[i].sin_port =  htons(port);
		if (inet_aton(dest_ip[i].c_str(), &addr_serv[i].sin_addr) == 0)
		{
			return -1;
		}
	}

	int concnt = 0;
	for (int i = 0; i < nodenum; i = (i+1)%nodenum)
	{
		if (conn_sock[i] > 0)
		{
			continue;
		}
		if (connect(sock_fd[i], (struct sockaddr *)&addr_serv[i], sizeof(struct sockaddr)) == 0)
		{
			conn_sock[i] = sock_fd[i];
			send(sock_fd[i], &node_id, sizeof(node_id), MSG_DONTWAIT);
			concnt++;
			//has built all the connection, exit here
			if (concnt == nodenum)
			{
				return 0;
			}
		}
	}

	return 0;
}
