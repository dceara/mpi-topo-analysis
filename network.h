/*
 * network.h
 *
 *  Created on: Nov 15, 2009
 *      Author: take
 */

#ifndef NETWORK_H_
#define NETWORK_H_

#include "perf_eval.h"

/*
 * Initializes the network layer. Requires pointers to the command line arguments
 * received by the application.
 * Returns 0 for success, 1 otherwise.
 */
int init_network(int* argcp, char*** argvp);

/*
 * Stores the network size in the 'size' parameter.
 * Returns 0 on success, 1 otherwise.
 */
int get_network_size(int* size);

/*
 * Stores the network rank in the 'rank' parameter.
 * Returns 0 on success, 1 otherwise.
 */
int get_network_rank(int* rank);

/*
 * Destroys the network layer. Should be the last thing called by the application.
 */
int finalize_network();

int init_topology(int proc_count);

/*
 * A wrapper for the MPI_Scatter function.
 * root - the rank of the sending task
 * rank - the rank of the function caller
 * sendbuf - the buffer to scatter (only significant at root)
 * size - the size of the data scattered to one node
 * recvbuf - the address of the receiving buffer
 * recvcount - the size of the receiving buffer
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int scatter(int root, int rank, int net_size, void* sendbuf, int size,
    void* recvbuf, int recvcount, PE* pe);

/*
 * A wrapper for the MPI_Scatterv function.
 * root - the rank of the sending task
 * rank - the rank of the function caller
 * sendbuf - the buffer to scatter (only significant at root)
 * sendcounts - integer array of length group_size (only significant at root)
 * recvbuf - the address of the receiving buffer
 * recvcount - the size of the receiving buffer
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int scatterv(int root, int rank, int net_size, void* sendbuf, int* sendcounts,
    void* recvbuf, int recvcount, int groupsize, PE* pe);

/*
 * A wrapper for the MPI_Gather function.
 * root - the rank of the receiving task
 * rank - the rank of the function caller
 * sendbuf - the sending buffer
 * sendcount - the size of the data being sent
 * recvbuf - the address the buffer where the data is received (only significant at root)
 * recvcount - number of elements for any single receive
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int gather(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int recvcount, PE* pe);

/*
 * A wrapper for the MPI_Gatherv function.
 * root - the rank of the receiving task
 * rank - the rank of the function caller
 * sendbuf - the sending buffer
 * sendcount - the size of the data being sent
 * recvbuf - the address the buffer where the data is received (only significant at root)
 * recvcounts - integer array of length group_size (only significant at root)
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int gatherv(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int* recvcounts, int groupsize, PE* pe);

/*
 * A wrapper for the MPI_Bcast function.
 * root - the rank of the task broadcasting
 * rank - the rank of the function caller
 * sendbuf - the buffer containing the data being sent
 * sendcount - the size of the sent data
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int broadcast(int root, int rank, int net_size, void* sendbuf, int sendcount, PE* pe);

#endif /* NETWORK_H_ */
