/*
 * network.h
 *
 *  Created on: Nov 15, 2009
 *      Author: take
 */

#ifndef NETWORK_H_
#define NETWORK_H_

typedef struct mpi_request Request;

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

/*
 * A wrapper for the MPI_Scatter function.
 * root - the rank of the sending task
 * sendbuf - the buffer to scatter (only significant at root)
 * size - the size of the data scattered to one node
 * recvbuf - the address of the receiving buffer
 * recvcount - the size of the receiving buffer
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int scatter(int root, void* sendbuf, int size, void* recvbuf, int recvcount);

/*
 * A wrapper for the MPI_Scatterv function.
 * root - the rank of the sending task
 * sendbuf - the buffer to scatter (only significant at root)
 * sendcounts - integer array of length group_size (only significant at root)
 * recvbuf - the address of the receiving buffer
 * recvcount - the size of the receiving buffer
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int scatterv(int root, void* sendbuf, int* sendcounts, void* recvbuf, int recvcount, int groupsize);

/*
 * A wrapper for the MPI_Gather function.
 * root - the rank of the receiving task
 * sendbuf - the sending buffer
 * sendcount - the size of the data being sent
 * recvbuf - the address the buffer where the data is received (only significant at root)
 * recvcount - number of elements for any single receive
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int gather(int root, void* sendbuf, int sendcount, void* recvbuf, int recvcount);

/*
 * A wrapper for the MPI_Gatherv function.
 * root - the rank of the receiving task
 * sendbuf - the sending buffer
 * sendcount - the size of the data being sent
 * recvbuf - the address the buffer where the data is received (only significant at root)
 * recvcounts - integer array of length group_size (only significant at root)
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int gatherv(int root, void* sendbuf, int sendcount, void* recvbuf, int* recvcounts, int groupsize);

/*
 * A wrapper for the MPI_Bcast function.
 * root - the rank of the task broadcasting
 * sendbuf - the buffer containing the data being sent
 * sendcount - the size of the sent data
 * Returns 0 on success, 1 otherwise.
 *
 * The function implementation is dependent on the virtual topology being used.
 */
int broadcast(int root, void* sendbuf, int sendcount);

#endif /* NETWORK_H_ */
