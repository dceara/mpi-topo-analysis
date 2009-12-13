/*
 * grid_network.c
 *
 *  Created on: Dec 3, 2009
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
    int tag, int net_size, int dest, PE* pe)
{
  int col = COL(grid_size, rank);
  int row = ROW(grid_size, rank);
  int dest_col = COL(grid_size, dest);
  int dest_row = ROW(grid_size, dest);

  if (DIST(grid_size, root, rank) % 2 != 0) {
    if (dest_row < row && abs(dest_col - col) < abs(dest_row - row)) {
      if (send_up(root, rank, net_size, 1)) {
        pe_send_message(pe, UP(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, UP(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message up.\n");
        return 0;
      }
    }
    if (dest_col < col && abs(dest_row - row) <= abs(dest_col - col)) {
      if (send_left(root, rank, net_size, 1)) {
        pe_send_message(pe, LEFT(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, LEFT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message left.\n");
        return 0;
      }
    }
    if (dest_row > row && abs(dest_col - col) < abs(dest_row - row)) {
      if (send_down(root, rank, net_size, 1)) {
        pe_send_message(pe, DOWN(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, DOWN(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message down.\n");
        return 0;
      }
    }
    if (dest_col > col && abs(dest_row - row) <= abs(dest_col - col)) {
      if (send_right(root, rank, net_size, 1)) {
        pe_send_message(pe, RIGHT(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, RIGHT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message right.\n");
      }
      return 0;
    }
  } else {
    if (dest_row < row && abs(dest_col - col) <= abs(dest_row - row)) {
      if (send_up(root, rank, net_size, 1)) {
        pe_send_message(pe, UP(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, UP(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message up.\n");
        return 0;
      }
    }
    if (dest_col < col && abs(dest_row - row) < abs(dest_col - col)) {
      if (send_left(root, rank, net_size, 1)) {
        pe_send_message(pe, LEFT(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, LEFT(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message left.\n");
        return 0;
      }
    }
    if (dest_row > row && abs(dest_col - col) <= abs(dest_row - row)) {
      if (send_down(root, rank, net_size, 1)) {
        pe_send_message(pe, DOWN(grid_size, rank), size);
        CHECK(MPI_Send(message, size, MPI_BYTE, DOWN(grid_size, rank), dest,
                MPI_COMM_WORLD) == MPI_SUCCESS, err,
            "forward_message: Unable to forward message down.\n");
        return 0;
      }
    }
    if (dest_col > col && abs(dest_row - row) < abs(dest_col - col)) {
      if (send_right(root, rank, net_size, 1)) {
        pe_send_message(pe, RIGHT(grid_size, rank), size);
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
    void* recvbuf, int recvcount, PE* pe)
{
  int i;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  if (root == rank) {
    for (i = 0; i < net_size; ++i) {
      if (i == rank)
        continue;
      CHECK(forward_message(root, rank, sendbuf + i * size, size, i, net_size, i, pe) == 0,
          err, "scatter: Unable to forward message as root.\n");
    }
    /* Store local message. */
    memcpy(recvbuf, sendbuf + rank * size, size);
  } else {
    MPI_Status status;

    CHECK((tmp = malloc(size * sizeof(*tmp))) != NULL || size != 0,
        err, "scatter: Out of memory!\n");
    for (;;) {
      pe_recv_message(pe, parent_rank, MPI_ANY_TAG);
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
              status.MPI_TAG, pe) == MPI_SUCCESS,
          err, "scatter: Unable to forward scatter message.\n");
    }
    if (tmp != NULL)
      free(tmp);
  }
  /* Announce neighbours that scattering has ended. */
  if (UP(grid_size, rank) != parent_rank && send_up(root, rank, net_size, 1)) {
    pe_send_message(pe, UP(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, UP(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce up neighbor of termination.\n");
  }

  if (LEFT(grid_size, rank) != parent_rank
      && send_left(root, rank, net_size, 1)) {
    pe_send_message(pe, LEFT(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, LEFT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce left neighbor of termination.\n");
  }

  if (DOWN(grid_size, rank) != parent_rank
      && send_down(root, rank, net_size, 1)) {
    pe_send_message(pe, DOWN(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, DOWN(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce down neighbor of termination.\n");
  }

  if (RIGHT(grid_size, rank) != parent_rank && send_right(root, rank, net_size,
      1)) {
    pe_send_message(pe, RIGHT(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, RIGHT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatter: Unable to announce right neighbor of termination.\n");
  }
  return 0;

  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int scatterv(int root, int rank, int net_size, void* sendbuf, int* sendcounts,
    void* recvbuf, int recvcount, int groupsize, PE* pe)
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
      CHECK(forward_message(root, rank, sendbuf + current_index, sendcounts[i], i,
              net_size, i, pe) == 0,
          err, "scatterv: Unable to forward message as root.\n");
      current_index += sendcounts[i];
    }
    /* Store local message. */
    memcpy(recvbuf, sendbuf + root_index, sendcounts[rank]);
  } else {
    MPI_Status status;

    for (;;) {
      pe_recv_message(pe, parent_rank, MPI_ANY_TAG);
      CHECK(MPI_Probe(parent_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatterv: Unable to probe for message from parent.\n");
      if (status.MPI_TAG == SCATTER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, parent_rank, SCATTER_TAG, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE) == MPI_SUCCESS,
            err, "scatterv: Unable to receive termination message from parent.\n")
        break;
      }

      CHECK((tmp = realloc(tmp, status._count * sizeof(*tmp))) != NULL || status._count == 0,
          err, "scatterv: Out of memory!\n");
      CHECK(MPI_Recv(tmp, status._count, MPI_BYTE, parent_rank, MPI_ANY_TAG,
              MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatterv: Unable to receive scatterv message from parent.\n")
      if (status.MPI_TAG == rank) {
        memcpy(recvbuf, tmp, recvcount);
      } else {
        CHECK(forward_message(root, rank, tmp, status._count, status.MPI_TAG,
                net_size, status.MPI_TAG, pe) == 0,
            err, "scatterv: Unable to forward scatterv message.\n");
      }
    }

  }
  /* Announce neighbours that scattering has ended. */
  if (UP(grid_size, rank) != parent_rank && send_up(root, rank, net_size, 1)) {
    pe_send_message(pe, UP(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, UP(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatterv: Unable to send termination message upwards.\n");
  }

  if (LEFT(grid_size, rank) != parent_rank
      && send_left(root, rank, net_size, 1)) {
    pe_send_message(pe, LEFT(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, LEFT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatterv: Unable to send termination message to the left.\n");
  }

  if (DOWN(grid_size, rank) != parent_rank
      && send_down(root, rank, net_size, 1)) {
    pe_send_message(pe, DOWN(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, DOWN(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatterv: Unable to send termination message downwards.\n");
  }

  if (RIGHT(grid_size, rank) != parent_rank && send_right(root, rank, net_size,
      1)) {
    pe_send_message(pe, RIGHT(grid_size, rank), 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, RIGHT(grid_size, rank), SCATTER_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "scatterv: Unable to send termination message to the right.\n");
  }

  if (tmp != NULL)
    free(tmp);

  return 0;
  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int gather(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int recvcount, PE* pe)
{
  MPI_Status status;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  if (rank != root)
    CHECK((tmp = malloc(sendcount * sizeof(*tmp))) != NULL || sendcount == 0,
        err, "gather: Out of memory!\n");

  if (send_up(root, rank, net_size, 1)) {
    int up = UP(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, up, MPI_ANY_TAG);
      CHECK(MPI_Probe(up, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to probe message from up neighbor.\n");
      if (status.MPI_TAG == GATHER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, up, GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive termination message from up.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE, up,
                MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message from up.\n");
      } else {
        CHECK(MPI_Recv(tmp, sendcount, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gather: Unable to recieve gather message to forward from up.\n");
        pe_send_message(pe, parent_rank, sendcount);
        CHECK(MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gather: Unable to forward gather message to parent.\n");
      }
    }
  }

  if (send_left(root, rank, net_size, 1)) {
    int left = LEFT(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, left, MPI_ANY_TAG);
      CHECK(MPI_Probe(left, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to probe message from left neighbor.\n");
      if (status.MPI_TAG == GATHER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, left, GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive termination message from left.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
                left, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message from left.\n");
      } else {
        CHECK(MPI_Recv(tmp, sendcount, MPI_BYTE, left, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message to forward from left.\n");
        pe_send_message(pe, parent_rank, sendcount);
        CHECK(MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gather: Unable to forward gather message to parent.\n");
      }
    }
  }

  if (send_down(root, rank, net_size, 1)) {
    int down = DOWN(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, down, MPI_ANY_TAG);
      CHECK(MPI_Probe(down, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to probe message from down neighbor.\n");
      if (status.MPI_TAG == GATHER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, down, GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive termination from down.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
                down, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message from down.\n");
      } else {
        CHECK(MPI_Recv(tmp, sendcount, MPI_BYTE, down, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message to forward from down.\n");
        pe_send_message(pe, parent_rank, sendcount);
        CHECK(MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gather: Unable to forward gather message to parent.\n");
      }
    }
  }

  if (send_right(root, rank, net_size, 1)) {
    int right = RIGHT(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, right, MPI_ANY_TAG);
      CHECK(MPI_Probe(right, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to probe message from right neighbor.\n");
      if (status.MPI_TAG == GATHER_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, right, GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive termination message from right.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + status.MPI_TAG * recvcount, recvcount, MPI_BYTE,
                right, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message from right.\n");
      } else {
        CHECK(MPI_Recv(tmp, sendcount, MPI_BYTE, right, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gather: Unable to receive gather message to forward from right.\n");
        pe_send_message(pe, parent_rank, sendcount);
        CHECK(MPI_Send(tmp, sendcount, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gather: Unable to forward gather message to parent.\n");
      }
    }
  }

  if (rank != root) {
    pe_send_message(pe, parent_rank, sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, parent_rank, rank, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gather: Unable to send gather message to parent.\n");

    pe_send_message(pe, parent_rank, 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, parent_rank, GATHER_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gather: Unable to send termination message to parent.\n");

    if (tmp != NULL)
      free(tmp);
  } else {
    memcpy(recvbuf + rank, sendbuf, recvcount);
  }
  return 0;

  err: if (tmp != NULL)
    free(tmp);
  return 1;
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
    void* recvbuf, int* recvcounts, int groupsize, PE* pe)
{
  MPI_Status status;
  char* tmp = NULL;
  int parent_rank = get_parent(root, rank, net_size);

  if (send_up(root, rank, net_size, 1)) {
    int up = UP(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, up, MPI_ANY_TAG);
      CHECK(MPI_Probe(up, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to probe for message from up.\n");
      if (status.MPI_TAG == GATHERV_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, up, GATHERV_TAG, MPI_COMM_WORLD, &status)== MPI_SUCCESS,
            err, "gatherv: Unable to receive termination message from up.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
                status._count, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive gatherv message from up.\n");
      } else {
        CHECK((tmp = realloc(tmp, status._count * sizeof(*tmp))) != NULL || status._count == 0,
            err, "gatherv: Out of memory!\n");
        CHECK(MPI_Recv(tmp, status._count, MPI_BYTE, up, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive message from up.\n");
        pe_send_message(pe, parent_rank, status._count);
        CHECK(MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gatherv: Unable to forward message to parent.\n");
      }
    }
  }

  if (send_left(root, rank, net_size, 1)) {
    int left = LEFT(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, left, MPI_ANY_TAG);
      CHECK(MPI_Probe(left, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to probe for message from left.\n");
      if (status.MPI_TAG == GATHERV_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, left, GATHERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive ending message from left.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
                status._count, MPI_BYTE, left, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive gatherv message from left.\n");
      } else {
        CHECK((tmp = realloc(tmp, status._count * sizeof(*tmp))) != NULL || status._count == 0,
            err, "gatherv: Out of memory!\n");
        CHECK(MPI_Recv(tmp, status._count, MPI_BYTE, left, MPI_ANY_TAG,
                MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive message to forward from left.\n");
        pe_send_message(pe, parent_rank, status._count);
        CHECK(MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gatherv: Unable to forward message to parent.\n");
      }
    }
  }

  if (send_down(root, rank, net_size, 1)) {
    int down = DOWN(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, down, MPI_ANY_TAG);
      CHECK(MPI_Probe(down, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to probe for message from down.\n");
      if (status.MPI_TAG == GATHERV_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, down, GATHERV_TAG, MPI_COMM_WORLD, &status)== MPI_SUCCESS,
            err, "gatherv: Unable to receive ending message from down.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
                status._count, MPI_BYTE, down, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive gatherv message from down.\n");
      } else {
        CHECK((tmp = realloc(tmp, status._count * sizeof(*tmp))) != NULL || status._count == 0,
            err, "gatherv: Out of memory!\n");
        CHECK(MPI_Recv(tmp, status._count, MPI_BYTE, down, MPI_ANY_TAG,
                MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive message to forward from down.\n");
        pe_send_message(pe, parent_rank, status._count);
        CHECK(MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gatherv: Unable to forward message to parent.\n");
      }
    }
  }

  if (send_right(root, rank, net_size, 1)) {
    int right = RIGHT(grid_size, rank);

    for (;;) {
      pe_recv_message(pe, right, MPI_ANY_TAG);
      CHECK(MPI_Probe(right, MPI_ANY_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to probe for message from right.\n");
      if (status.MPI_TAG == GATHERV_TAG) {
        CHECK(MPI_Recv(NULL, 0, MPI_BYTE, right, GATHERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive ending message from right.\n");
        break;
      }
      if (root == rank) {
        CHECK(MPI_Recv(recvbuf + get_offset(status.MPI_TAG, recvcounts),
                status._count, MPI_BYTE, right, MPI_ANY_TAG, MPI_COMM_WORLD,
                &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive gatherv message from right.\n");
      } else {
        CHECK((tmp = realloc(tmp, status._count * sizeof(*tmp))) != NULL || status._count == 0,
            err, "gatherv: Out of memory!\n");
        CHECK(MPI_Recv(tmp, status._count, MPI_BYTE, right, MPI_ANY_TAG,
                MPI_COMM_WORLD, &status) == MPI_SUCCESS,
            err, "gatherv: Unable to receive message to forward from right.\n");
        pe_send_message(pe, parent_rank, status._count);
        CHECK(MPI_Send(tmp, status._count, MPI_BYTE, parent_rank, status.MPI_TAG,
                MPI_COMM_WORLD)== MPI_SUCCESS,
            err, "gatherv: Unable to forward message to parent.\n");
      }
    }
  }

  if (rank != root) {
    pe_send_message(pe, parent_rank, sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, parent_rank,
            rank, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gatherv: Unable to send gatherv message to parent.\n");

    pe_send_message(pe, parent_rank, 0);
    CHECK(MPI_Send(NULL, 0, MPI_BYTE, parent_rank,
            GATHERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gatherv: Unable to send ending message to parent.\n");

    if (tmp != NULL)
      free(tmp);
  } else {
    memcpy(recvbuf + get_offset(rank, recvcounts), sendbuf, recvcounts[rank]);
  }
  return 0;

  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int broadcast(int root, int rank, int net_size, void* sendbuf, int sendcount, PE* pe)
{
  if (rank != root) {
    pe_recv_message(pe, MPI_ANY_SOURCE, BCAST_TAG);
    CHECK(MPI_Recv(sendbuf, sendcount, MPI_INT, MPI_ANY_SOURCE, BCAST_TAG,
            MPI_COMM_WORLD, MPI_STATUS_IGNORE) == MPI_SUCCESS,
        err, "broadcast: Unable to receive broadcast message.\n");
  }

  if (send_up(root, rank, net_size, 1)) {
    pe_send_message(pe, UP(grid_size, rank), sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, UP(grid_size, rank), BCAST_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "broadcast: Unable to send broadcast message up.\n");
  }

  if (send_left(root, rank, net_size, 1)) {
    pe_send_message(pe, LEFT(grid_size, rank), sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, LEFT(grid_size, rank), BCAST_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "broadcast: Unable to send broadcast message left.\n");
  }

  if (send_down(root, rank, net_size, 1)) {
    pe_send_message(pe, DOWN(grid_size, rank), sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, DOWN(grid_size, rank), BCAST_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "broadcast: Unable to send broadcast message down.\n");
  }

  if (send_right(root, rank, net_size, 1)) {
    pe_send_message(pe, RIGHT(grid_size, rank), sendcount);
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, RIGHT(grid_size, rank), BCAST_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "broadcast: Unable to send broadcast message right.\n");
  }
  return 0;

  err: return 1;
}
