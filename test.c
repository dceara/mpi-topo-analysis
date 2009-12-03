/*
 * test.c
 *
 *  Created on: Dec 2, 2009
 *      Author: take
 */
#include <mpi.h>
#include "network.h"
#include "utils.h"

#define SIZE 6
#define ROOT 0

int main(int argc, char** argv)
{
  int size, rank;

  char value[SIZE] = "012345";
  char buf[SIZE] = "empty";
  int sizes[5] = {0, 2, 1, 2, 0};

  int get_offset(int rank) {
    int offset = 0;
    int i;

    for (i = 0; i < rank; ++ i)
      offset += sizes[i];
    return offset;
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == ROOT) {
    PRINT("rank %d: Calling gatherv as root.\n", rank);
    gatherv(rank, rank, size, value + get_offset(rank), sizes[rank], buf, sizes, size);
    //gather(rank, rank, size, value + rank, 1, buf, 1);
    //scatterv(rank, rank, size, value, sizes, buf, sizes[rank], size);
    //scatter(rank, rank, size, value, 1, buf, 1);
    //broadcast(rank, rank, size, bcast_value, SIZE);
    PRINT("rank %d: I received the value: %s.\n", rank, buf);
  } else {
    PRINT("rank %d: Calling gatherv as non-root.\n", rank);
    gatherv(ROOT, rank, size, value + get_offset(rank), sizes[rank], NULL, NULL, size);
    //gather(ROOT, rank, size, value + rank, 1, NULL, 0);
    //scatterv(1, rank, size, NULL, NULL, buf, sizes[rank], size);
    //scatter(0, rank, size, NULL, 0, buf, 1);
    //broadcast(0, rank, size, buf, SIZE);
  }

  MPI_Finalize();
  return 0;
}
