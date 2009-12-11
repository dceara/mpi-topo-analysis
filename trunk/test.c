/*
 * test.c
 *
 *  Created on: Dec 2, 2009
 *      Author: take
 */
#include <mpi.h>
#include "network.h"
#include "utils.h"

#define SIZE 17
#define ROOT 0

int main(int argc, char** argv)
{
  int size, rank;

  char value[SIZE] = "0123456789012345";
  char buf[SIZE] = "empty00000000000";
  int sizes[SIZE] =
  { 1, 1, 1, 2, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
  int i;

  int get_offset(int rank)
  {
    int offset = 0;
    int i;

    for (i = 0; i < rank; ++i)
      offset += sizes[i];
    return offset;
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  init_topology(size);

  for (i = 0; i < 10000; ++i) {
    if (rank == i % size) {
      PRINT("rank %d: Calling scatter as root i = %d.\n", rank, i);
      //gatherv(rank, rank, size, value + get_offset(rank), sizes[rank], buf, sizes, size);
      gather(i % size, rank, size, value + rank, 1, buf, 1);
      //scatterv(i % size, rank, size, value, sizes, buf, sizes[rank], size);
      //scatter(rank, rank, size, value, 1, buf, 1);
      //broadcast(rank, rank, size, value, SIZE);
      PRINT("rank %d: I received the value: %s i = %d.\n", rank, buf, i);
    } else {
      PRINT("rank %d: Calling scatter as non-root i = %d.\n", rank, i);
      //gatherv(ROOT, rank, size, value + get_offset(rank), sizes[rank], NULL, NULL, size);
      gather(i % size, rank, size, value + rank, 1, NULL, 0);
      //scatterv(i % size, rank, size, NULL, NULL, buf, sizes[rank], size);
      //scatter(i % size, rank, size, NULL, 0, buf, 1);
      //broadcast(0, rank, size, buf, SIZE);
      //PRINT("rank %d: I received the value: %s i = %d.\n", rank, buf, i);
    }
    //PRINT("rank %d: i = %d\n", rank, i);
  }

  MPI_Finalize();
  return 0;
}
