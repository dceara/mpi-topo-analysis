/*
 * test.c
 *
 *  Created on: Dec 2, 2009
 *      Author: take
 */
#include <mpi.h>
#include "network.h"
#include "utils.h"
#include "perf_eval.h"

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
  PE* pe;

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

  pe = init_perf_eval(rank);

  for (i = 0; i < 10000; ++i) {
    if (rank == i % size) {
      PRINT("rank %d: Calling gather as root i = %d.\n", rank, i);
      gatherv(i % size, rank, size, value + get_offset(rank), sizes[rank], buf, sizes, size, pe);
      gather(i % size, rank, size, value + rank, 1, buf, 1, pe);
      scatterv(i % size, rank, size, value, sizes, buf, sizes[rank], size, pe);
      scatter(i % size, rank, size, value, 1, buf, 1, pe);
      broadcast(i % size, rank, size, value, SIZE, pe);
      PRINT("rank %d: I received the value: %s i = %d.\n", rank, buf, i);
    } else {
      PRINT("rank %d: Calling gather as non-root i = %d.\n", rank, i);
      gatherv(i % size, rank, size, value + get_offset(rank), sizes[rank], NULL, NULL, size, pe);
      gather(i % size, rank, size, value + rank, 1, NULL, 0, pe);
      scatterv(i % size, rank, size, NULL, NULL, buf, sizes[rank], size, pe);
      scatter(i % size, rank, size, NULL, 0, buf, 1, pe);
      broadcast(i % size, rank, size, buf, SIZE, pe);
      //PRINT("rank %d: I received the value: %s i = %d.\n", rank, buf, i);
    }
    //PRINT("rank %d: i = %d\n", rank, i);
  }

  MPI_Finalize();
  return 0;
}
