/*
 * default_network.c
 *
 *  Created on: Nov 15, 2009
 *      Author: take
 */

/* This is a default implementation of the network layer.
 * It uses directly the MPI primitives without taking into account the virtual topology.
 */

#include <mpi.h>
#include <stdlib.h>
#include "utils.h"
#include "network.h"

inline int scatter(int root, void* sendbuf, int* sendcounts, void* recvbuf,
    int recvcount, int groupsize)
{
  int* displ;

  CHECK((displ = calloc(groupsize, sizeof(*displ))) != NULL, alloc_err,
      "scatter: Out of memory!\n");
  CHECK(MPI_Scatterv(sendbuf, sendcounts, displ, MPI_BYTE,
          recvbuf, recvcount, MPI_BYTE, root, MPI_COMM_WORLD) == MPI_SUCCESS,
      scatter_err, "scatter: Error when calling MPI_Scatterv.\n");
  free(displ);
  return 0;

scatter_err:
  free(displ);
alloc_err:
  return 1;
}

inline int gather(int root, void* sendbuf, int sendcount, void* recvbuf,
    int* recvcounts, int groupsize)
{
  int* displ;

  CHECK((displ = calloc(groupsize, sizeof(*displ))) != NULL, alloc_err,
      "gather: Out of memory!\n");
  CHECK(MPI_Gatherv(sendbuf, sendcount, MPI_BYTE, recvbuf,
          recvcounts, displ, MPI_BYTE, root, MPI_COMM_WORLD) == MPI_SUCCESS,
      gather_err, "gather: Error when calling MPI_Gatherv.\n")
  free(displ);
  return 0;

gather_err:
  free(displ);
alloc_err:
  return 1;
}

inline int broadcast(int root, void* sendbuf, int sendcount)
{
  CHECK(MPI_Bcast(sendbuf, sendcount, MPI_BYTE, root, MPI_COMM_WORLD),
      bcast_err, "broadcast: Error when calling MPI_Bcast.\n");
  return 0;

bcast_err:
  return 1;
}
