/*
 * network.c
 *
 *  Created on: Nov 15, 2009
 *      Author: take
 */

#include <mpi.h>
#include "network.h"
#include "utils.h"

inline int init_network(int* argcp, char*** argvp)
{
  CHECK(MPI_Init(argcp, argvp) == MPI_SUCCESS, mpi_err,
      "init_network: Unable to initialize MPI.\n");
  return 0;
mpi_err:
  return 1;
}

inline int get_network_size(int* size)
{
  CHECK(MPI_Comm_size(MPI_COMM_WORLD, size) == MPI_SUCCESS, mpi_err,
      "get_network_size: Unable to retrieve network size from MPI.\n")
  return 0;
mpi_err:
  return 1;
}

inline int get_network_rank(int* rank)
{
  CHECK(MPI_Comm_rank(MPI_COMM_WORLD, rank) == MPI_SUCCESS, mpi_err,
      "get_network_size: Unable to retrieve network rank from MPI.\n")
  return 0;
mpi_err:
  return 1;
}

inline int finalize_network()
{
  return MPI_Finalize();
}
