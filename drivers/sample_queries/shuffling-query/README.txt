The sample query involves two nodes, node 0 sending data to node 1.

To run the query, use the following steps:
1. Edit the file "nodesend_0.conf": change "10.1.1.1" to the IP address of node 0 and change "10.1.1.2" to the IP address of node 1.
2. Do the same for "noderecv_1.conf".
3. In node 0, run the command "./drivers/executequery.no-mpi genR0.conf" to generate the data to be sent out.
4. In node 0, run the command "./drivers/executequery.no-mpi nodesend_0.conf".
5. In node 1, run the command "./drivers/executequery.no-mpi noderecv_1.conf".

For how to configure the parameters for the operators, refer to the Doxygen comments in the file operators/operators.h
