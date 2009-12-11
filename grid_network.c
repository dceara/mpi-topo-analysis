/*
 * grid_network.c
 *
 *  Created on: Dec 3, 2009
 *      Author: take
 */

/*
 * ring_network.c
 *
 *  Created on: Dec 2, 2009
 *      Author: take
 */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "utils.h"
#include "network.h"

#define ROW(n, rank) \
  (rank / n)
#define COL(n, rank) \
  (rank % n)
#define LEFT(n, rank) \
  (rank - 1)
#define RIGHT(n, rank) \
  (rank + 1)
#define UP(n, rank) \
  (rank - n)
#define DOWN(n, rank) \
  (rank + n)
#define DIST(n, first, second) \
  (abs(ROW(n, first) - ROW(n, second)) \
      + abs(COL(n, first) - COL(n, second)))

static int BCAST_TAG;
static int SCATTER_TAG;
static int SCATTERV_TAG;
static int GATHER_TAG;
static int GATHERV_TAG;

static int grid_size = 0;

int init_topology(int proc_count)
{
  grid_size = (int) ceil(sqrt(proc_count));
  BCAST_TAG = proc_count + 1;
  SCATTER_TAG = proc_count + 2;
  SCATTERV_TAG = proc_count + 3;
  GATHER_TAG = proc_count + 4;
  GATHERV_TAG = proc_count + 5;
  return 0;
}

static inline int get_parent(int root, int rank, int net_size)
{
  int root_col = COL(grid_size, root);
  int root_row = ROW(grid_size, root);
  int col = COL(grid_size, rank);
  int row = ROW(grid_size, rank);

  if (root_col == col && root_row == row)
    return -1;
  if (root_col == col && root_row > row)
    return DOWN(grid_size, rank);
  if (root_col == col && root_row < row)
    return UP(grid_size, rank);
  if (root_row == row && root_col > col)
    return RIGHT(grid_size, rank);
  if (root_row == row && root_col < col)
    return LEFT(grid_size, rank);

  if (root_row > row && root_col > col && DIST(grid_size, root, rank) % 2 != 0)
    return DOWN(grid_size, rank);
  if (root_row > row && root_col > col && DIST(grid_size, root, rank) % 2 == 0)
    return RIGHT(grid_size, rank);

  if (root_row > row && root_col < col && DIST(grid_size, root, rank) % 2 != 0)
    return DOWN(grid_size, rank);
  if (root_row > row && root_col < col && DIST(grid_size, root, rank) % 2 == 0)
    return LEFT(grid_size, rank);

  if (root_row < row && root_col > col && DIST(grid_size, root, rank) % 2 != 0)
    return UP(grid_size, rank);
  if (root_row < row && root_col > col && DIST(grid_size, root, rank) % 2 == 0)
    return RIGHT(grid_size, rank);

  if (root_row < row && root_col < col && DIST(grid_size, root, rank) % 2 != 0)
    return UP(grid_size, rank);
  if (root_row < row && root_col < col && DIST(grid_size, root, rank) % 2 == 0)
    return LEFT(grid_size, rank);
  assert(0);
  return -1;
}

#warning not sure if it works for proc_count != perfect square
#warning implement incoming functions

static inline int send_left(int root, int rank, int net_size, int outgoing)
{
  if (COL(grid_size, rank) == 0)
    return 0;
  if (outgoing) {
    if (COL(grid_size, rank) <= COL(grid_size, root) && (ROW(grid_size, rank)
        == ROW(grid_size, root) || DIST(grid_size, root, rank) % 2 != 0))
      return 1;
  }
  return 0;
}

static inline int send_right(int root, int rank, int net_size, int outgoing)
{
  if (COL(grid_size, rank) == grid_size - 1)
    return 0;
  if (RIGHT(grid_size, rank) >= net_size)
    return 0;
  if (outgoing) {
    if (COL(grid_size, rank) >= COL(grid_size, root) && (ROW(grid_size, rank)
        == ROW(grid_size, root) || DIST(grid_size, root, rank) % 2 != 0))
      return 1;
  }
  return 0;
}

static inline int send_up(int root, int rank, int net_size, int outgoing)
{
  if (ROW(grid_size, rank) == 0)
    return 0;
  if (outgoing) {
    if (ROW(grid_size, rank) <= ROW(grid_size, root) && (COL(grid_size, rank)
        == COL(grid_size, root) || DIST(grid_size, root, rank) % 2 == 0))
      return 1;
  }
  return 0;
}

static inline int send_down(int root, int rank, int net_size, int outgoing)
{
  if (ROW(grid_size, rank) == grid_size - 1)
    return 0;
  if (DOWN(grid_size, rank) >= net_size)
    return 0;

  if (outgoing) {
    if (ROW(grid_size, rank) >= ROW(grid_size, root) && (COL(grid_size, rank)
        == COL(grid_size, root) || DIST(grid_size, root, rank) % 2 == 0))
      return 1;
  }
  return 0;
}

static inline int forward_message(int root, int rank, char* message, int size,
    int tag, int net_size, int dest)
{
  int col = COL(grid_size, rank);
  int row = ROW(grid_size, rank);
  int dest_col = COL(grid_size, dest);
  int dest_row = ROW(grid_size, dest);

  if (DIST(grid_size, root, rank) % 2 != 0) {
    if (dest_row < row && abs(dest_col - col) < abs(dest_row - row)) {
      if (send_up(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, UP(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message up.\n");
        return 0;
      }
    }
    if (dest_col < col && abs(dest_row - row) <= abs(dest_col - col)) {
      if (send_left(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, LEFT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message left.\n");
        return 0;
      }
    }
    if (dest_row > row && abs(dest_col - col) < abs(dest_row - row)) {
      if (send_down(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, DOWN(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message down.\n");
        return 0;
      }
    }
    if (dest_col > col && abs(dest_row - row) <= abs(dest_col - col)) {
      if (send_right(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, RIGHT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message right.\n");
      }
      return 0;
    }
  } else {
    if (dest_row < row && abs(dest_col - col) <= abs(dest_row - row)) {
      if (send_up(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, UP(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message up.\n");
        return 0;
      }
    }
    if (dest_col < col && abs(dest_row - row) < abs(dest_col - col)) {
      if (send_left(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, LEFT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message left.\n");
        return 0;
      }
    }
    if (dest_row > row && abs(dest_col - col) <= abs(dest_row - row)) {
      if (send_down(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, DOWN(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message down.\n");
        return 0;
      }
    }
    if (dest_col > col && abs(dest_row - row) < abs(dest_col - col)) {
      if (send_right(root, rank, net_size, 1)) {
        CHECK(MPI_Send(message, size, MPI_BYTE, RIGHT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message right.\n");
      }
      return 0;
    }
  }
  assert(0);
  err: return 1;
}

int scatter(int root, int rank, int net_size, void* sendbuf, int size,
    void* recvbuf, int recvcount)
{
  int i;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  if (root == rank) {
    for (i = 0; i < net_size; ++i) {
      if (i == rank)
        continue;
      CHECK(forward_message(root, rank, sendbuf + i * size, size, i, net_size, i) == 0,
          err, "scatter: Unable to forward message as root.\n");
    }
    /* Store local message. */
    memcpy(recvbuf, sendbuf + rank * size, size);
  } else {
    MPI_Status status;

    CHECK((tmp = malloc(size * sizeof(*tmp))) != NULL || size != 0,
        err, "scatter: Out of memory!\n");
    for (;;) {
      CHECK(MPI_Probe(parent_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatter: Unable to probe the network for messages from parent\n");
      if (status.MPI_TAG == SCATTER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, parent_rank, SCATTER_TAG, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE) == MPI_SUCCESS,
            err, "scatter: Unable to receive ending message from parent.\n");
        break;
      }
      CHECK(MPI_Recv(tmp, recvcount, MPI_BYTE, parent_rank, MPI_ANY_TAG,
              MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatter: Unable to receive scatter message from parent.\n");
      if (status.MPI_TAG == rank)
        memcpy(recvbuf, tmp, recvcount);
      else CHECK(forward_message(root, rank, tmp, recvcount, status.MPI_TAG, net_size,
              status.MPI_TAG) == MPI_SUCCESS,
          err, "scatter: Unable to forward scatter message.\n");
    }
    free(tmp);
  }
  /* Announce neighbours that scattering has ended. */
  if (UP(grid_size, rank) != parent_rank && send_up(root, rank, net_size, 1))
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, UP(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce up neighbor of termination.\n");

  if (LEFT(grid_size, rank) != parent_rank
      && send_left(root, rank, net_size, 1))
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, LEFT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce left neighbor of termination.\n");

  if (DOWN(grid_size, rank) != parent_rank
      && send_down(root, rank, net_size, 1))
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, DOWN(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce down neighbor of termination.\n");

  if (RIGHT(grid_size, rank) != parent_rank && send_right(root, rank, net_size,
      1))
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, RIGHT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce right neighbor of termination.\n");

  return 0;

  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int scatterv(int root, int rank, int net_size, void* sendbuf, int* sendcounts,
    void* recvbuf, int recvcount, int groupsize)
{
  int i;
  int parent_rank = get_parent(root, rank, net_size);
  char* tmp = NULL;

  if (root == rank) {
    int current_index = 0;
    int root_index;

    for (i = 0; i < net_size; ++i) {
      if (i == rank) {
        root_index = current_index;
        current_index += sendcounts[i];
        continue;
      }
      forward_message(root, rank, sendbuf + current_index, sendcounts[i], i,
          net_size, i);
      current_index += sendcounts[i];
    }
    /* Store local message. */
    memcpy(recvbuf, sendbuf + root_index, sendcounts[rank]);
  } else {
    MPI_Status status;

    for (;;) {
      MPI_Probe(parent_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == SCATTER_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, parent_rank, SCATTER_TAG, MPI_COMM_WORLD,
            MPI_STATUS_IGNORE);
        break;
      }

      tmp = malloc(status._count * sizeof(*tmp));
      MPI_Recv(tmp, status._count, MPI_BYTE, parent_rank, MPI_ANY_TAG,
          MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == rank) {
        memcpy(recvbuf, tmp, recvcount);
      } else {
        forward_message(root, rank, tmp, status._count, status.MPI_TAG,
            net_size, status.MPI_TAG);
      }
      free(tmp);
    }

  }
  /* Announce neighbours that scattering has ended. */
  if (UP(grid_size, rank) != parent_rank && send_up(root, rank, net_size, 1)) {
    MPI_Send(NULL, 0, MPI_BYTE, UP(grid_size, rank), SCATTER_TAG,
        MPI_COMM_WORLD);
  }
  if (LEFT(grid_size, rank) != parent_rank
      && send_left(root, rank, net_size, 1)) {
    MPI_Send(NULL, 0, MPI_BYTE, LEFT(grid_size, rank), SCATTER_TAG,
        MPI_COMM_WORLD);
  }
  if (DOWN(grid_size, rank) != parent_rank
      && send_down(root, rank, net_size, 1)) {
    MPI_Send(NULL, 0, MPI_BYTE, DOWN(grid_size, rank), SCATTER_TAG,
        MPI_COMM_WORLD);
  }
  if (RIGHT(grid_size, rank) != parent_rank && send_right(root, rank, net_size,
      1)) {
    MPI_Send(NULL, 0, MPI_BYTE, RIGHT(grid_size, rank), SCATTER_TAG,
        MPI_COMM_WORLD);
  }
  return 0;
  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int gather(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int recvcount)
{
  MPI_Status status;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  DBG_PRINT("rank %d: parent_rank = %d.\n", rank, parent_rank);

  if (rank != root)
    tmp = malloc(sendcount * sizeof(*tmp));

  if (send_up(root, rank, net_size, 1)) {
    int up = UP(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from up.\n", rank);

    for (;;) {
      MPI_Probe(up, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHER_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, up, GATHER_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE, up,
            MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        MPI_Recv(tmp, sendcount, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
        MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_left(root, rank, net_size, 1)) {
    int left = LEFT(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from left.\n", rank);

    for (;;) {
      MPI_Probe(left, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHER_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, left, GATHER_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
            left, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        MPI_Recv(tmp, sendcount, MPI_BYTE, left, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
        MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_down(root, rank, net_size, 1)) {
    int down = DOWN(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from down.\n", rank);

    for (;;) {
      MPI_Probe(down, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHER_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, down, GATHER_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
            down, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        MPI_Recv(tmp, sendcount, MPI_BYTE, down, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
        MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_right(root, rank, net_size, 1)) {
    int right = RIGHT(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from right.\n", rank);

    for (;;) {
      MPI_Probe(right, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHER_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, right, GATHER_TAG, MPI_COMM_WORLD, &status);
        DBG_PRINT("rank %d: received termination message from right\n", rank);
        break;
      }DBG_PRINT("rank %d: receiving gather message from right\n", rank);
      if (root == rank) {
        MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
            right, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        MPI_Recv(tmp, sendcount, MPI_BYTE, right, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
        MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (rank != root) {
    DBG_PRINT("rank %d: sending own message to parent.\n", rank);
    MPI_Send(sendbuf, sendcount, MPI_BYTE, parent_rank, rank, MPI_COMM_WORLD);

    DBG_PRINT("rank %d: announcing parent of termination.\n", rank);
    MPI_Send(NULL, 0, MPI_BYTE, parent_rank, GATHER_TAG, MPI_COMM_WORLD);
    DBG_PRINT("rank %d: announced parent of termination.\n", rank);

    free(tmp);
  } else {
    memcpy(recvbuf + rank, sendbuf, recvcount);
  }
  return 0;
}

static inline int get_offset(int index, const int* const counts)
{
  int offset = 0;
  int i;

  for (i = 0; i < index; ++i)
    offset += counts[i];
  return offset;
}

int gatherv(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int* recvcounts, int groupsize)
{
  MPI_Status status;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  DBG_PRINT("rank %d: parent_rank = %d.\n", rank, parent_rank);

  if (send_up(root, rank, net_size, 1)) {
    int up = UP(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from up.\n", rank);

    for (;;) {
      MPI_Probe(up, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHERV_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, up, GATHERV_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
            status._count, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        tmp = realloc(tmp, status._count * sizeof(*tmp));
        MPI_Recv(tmp, status._count, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
        MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_left(root, rank, net_size, 1)) {
    int left = LEFT(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from left.\n", rank);

    for (;;) {
      MPI_Probe(left, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHERV_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, left, GATHERV_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
            status._count, MPI_BYTE, left, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        tmp = realloc(tmp, status._count * sizeof(*tmp));
        MPI_Recv(tmp, status._count, MPI_BYTE, left, MPI_ANY_TAG,
            MPI_COMM_WORLD, &status);
        MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_down(root, rank, net_size, 1)) {
    int down = DOWN(grid_size, rank);

    DBG_PRINT("rank %d: receiving gather messages from down.\n", rank);

    for (;;) {
      MPI_Probe(down, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHERV_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, down, GATHERV_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
            status._count, MPI_BYTE, down, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      } else {
        tmp = realloc(tmp, status._count * sizeof(*tmp));
        MPI_Recv(tmp, status._count, MPI_BYTE, down, MPI_ANY_TAG,
            MPI_COMM_WORLD, &status);
        MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (send_right(root, rank, net_size, 1)) {
    int right = RIGHT(grid_size, rank);

    for (;;) {
      MPI_Probe(right, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      if (status.MPI_TAG == GATHERV_TAG) {
        MPI_Recv(NULL, 0, MPI_BYTE, right, GATHERV_TAG, MPI_COMM_WORLD, &status);
        break;
      }
      if (root == rank) {
        MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
            status._count, MPI_BYTE, right, MPI_ANY_TAG, MPI_COMM_WORLD,
            &status);
      } else {
        tmp = realloc(tmp, status._count * sizeof(*tmp));
        MPI_Recv(tmp, status._count, MPI_BYTE, right, MPI_ANY_TAG,
            MPI_COMM_WORLD, &status);
        MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
            MPI_COMM_WORLD);
      }
    }
  }

  if (rank != root) {
    DBG_PRINT("rank %d: sending own message to parent.\n", rank);
    MPI_Send(sendbuf, sendcount, MPI_BYTE, parent_rank, rank, MPI_COMM_WORLD);

    DBG_PRINT("rank %d: announcing parent of termination.\n", rank);
    MPI_Send(NULL, 0, MPI_BYTE, parent_rank, GATHERV_TAG, MPI_COMM_WORLD);
    DBG_PRINT("rank %d: announced parent of termination.\n", rank);

    if (tmp != NULL)
      free(tmp);
  } else {
    memcpy(recvbuf + get_offset(rank, recvcounts), sendbuf, recvcounts[rank]);
  }
  return 0;
}

int broadcast(int root, int rank, int net_size, void* sendbuf, int sendcount)
{
#warning PERFORM CHECKS
  if (rank != root) {
    MPI_Recv(sendbuf, sendcount, MPI_INT, MPI_ANY_SOURCE, BCAST_TAG,
        MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }
  if (send_up(root, rank, net_size, 1)) {
    MPI_Send(sendbuf, sendcount, MPI_BYTE, UP(grid_size, rank), BCAST_TAG,
        MPI_COMM_WORLD);
  }
  if (send_left(root, rank, net_size, 1)) {
    MPI_Send(sendbuf, sendcount, MPI_BYTE, LEFT(grid_size, rank), BCAST_TAG,
        MPI_COMM_WORLD);
  }
  if (send_down(root, rank, net_size, 1)) {
    MPI_Send(sendbuf, sendcount, MPI_BYTE, DOWN(grid_size, rank), BCAST_TAG,
        MPI_COMM_WORLD);
  }
  if (send_right(root, rank, net_size, 1)) {
    MPI_Send(sendbuf, sendcount, MPI_BYTE, RIGHT(grid_size, rank), BCAST_TAG,
        MPI_COMM_WORLD);
  }
  return 0;
}
