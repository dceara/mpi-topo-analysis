/*
 * ring_network.c
 *
 *  Created on: Dec 2, 2009
 *      Author: take
 */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include "utils.h"
#include "network.h"

static int BCAST_TAG = 1;
static int SCATTER_TAG = 2;
static int SCATTERV_TAG = 3;
static int GATHER_TAG = 4;
static int GATHERV_TAG = 5;

int scatter(int root, int rank, int net_size, void* sendbuf, int size,
    void* recvbuf, int recvcount)
{
  MPI_Status status;
  int i;
  int next = (rank + 1) % net_size;

  if (root == rank) {
    for (i = net_size - 1; i >= 1; --i) {
      int next_index = (rank + i) % net_size;

      CHECK(MPI_Send((char* )sendbuf + next_index * size, size, MPI_BYTE, next,
              SCATTER_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatter: Error when scattering as root.\n");
    }
    memcpy(recvbuf, sendbuf + rank * size, size);
  } else {
    int prev = rank > 0 ? rank - 1 : net_size - 1;
    int fw_cnt = root - rank > 0 ? root - rank - 1 : (net_size + root - rank
        - 1) % net_size;

    MPI_Recv(recvbuf, recvcount, MPI_BYTE, prev, SCATTER_TAG, MPI_COMM_WORLD,
        &status);
    for (i = 0; i < fw_cnt; ++i) {
      CHECK(MPI_Send(recvbuf, recvcount, MPI_BYTE, next,
              SCATTER_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatter: Error when forwarding scatter values.\n");
      CHECK(MPI_Recv(recvbuf, recvcount, MPI_BYTE, prev,
              SCATTER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatter: Error when receiving scatter values.\n");
    }
  }
  return 0;

  err: return 1;
}

static inline int get_offset(int index, const int* const counts)
{
  int offset = 0;
  int i;

  for (i = 0; i < index; ++i)
    offset += counts[i];
  return offset;
}

int scatterv(int root, int rank, int net_size, void* sendbuf, int* sendcounts,
    void* recvbuf, int recvcount, int groupsize)
{
  MPI_Status status;
  int i;
  int next = (rank + 1) % net_size;
  char* tmp = NULL;

  if (root == rank) {
    for (i = net_size - 1; i >= 1; --i) {
      int next_index = (rank + i) % net_size;
      int next_offset = get_offset(next_index, sendcounts);

      CHECK(MPI_Send(&sendcounts[next_index], sizeof(*sendcounts),
              MPI_BYTE, next, SCATTERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatterv: Error when scatteringv as root.\n");

      CHECK(MPI_Send((char* )sendbuf + next_offset, sendcounts[next_index], MPI_BYTE, next,
              SCATTERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatterv: Error when scattering as root.\n");
    }
    memcpy(recvbuf, sendbuf + get_offset(rank, sendcounts), sendcounts[rank]);
  } else {
    int message_size;
    int prev = rank > 0 ? rank - 1 : net_size - 1;
    int fw_cnt = root - rank > 0 ? root - rank - 1 : (net_size + root - rank
        - 1) % net_size;

    CHECK(MPI_Recv(&message_size, sizeof(message_size), MPI_BYTE, prev,
            SCATTERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
        err, "scatterv: Error when forwarding scatter values.\n");
    CHECK((tmp = malloc(message_size * sizeof(*tmp))) != NULL || message_size == 0,
        err, "scatterv: Out of memory!\n");
    CHECK(MPI_Recv(tmp, message_size, MPI_BYTE, prev, SCATTERV_TAG, MPI_COMM_WORLD,
            &status) == MPI_SUCCESS,
        err, "scatterv: Error when receiving scatter values.\n");
    for (i = 0; i < fw_cnt; ++i) {
      CHECK(MPI_Send(&message_size, sizeof(message_size), MPI_BYTE,
              next, SCATTERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatter: Error when forwarding scatter values.\n");
      CHECK(MPI_Send(tmp, message_size, MPI_BYTE, next,
              SCATTERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "scatter: Error when forwarding scatter values.\n");

      CHECK(MPI_Recv(&message_size, sizeof(message_size), MPI_BYTE, prev,
              SCATTERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatter: Error when receiving scatter values.\n");
      CHECK((tmp = realloc(tmp, message_size * sizeof(*tmp))) != NULL || message_size == 0,
          err, "scatterv: Out of memory1\n");
      CHECK(MPI_Recv(tmp, message_size, MPI_BYTE, prev,
              SCATTERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "scatter: Error when receiving scatter values.\n");
    }
    memcpy(recvbuf, tmp, recvcount);
    free(tmp);
  }
  return 0;

  err: if (tmp != NULL) free(tmp);
  return 1;
}

int gather(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int recvcount)
{

  MPI_Status status;
  int i;
  int prev = rank > 0 ? rank - 1 : net_size - 1;
  char* tmp = NULL;

  if (root == rank) {
    for (i = 1; i <= net_size - 1; ++i) {
      int next_index = (rank + net_size - i) % net_size;

      CHECK(MPI_Recv(recvbuf + next_index * recvcount, recvcount, MPI_BYTE, prev,
              GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to receive gather messages as root.\n");
    }
    memcpy(recvbuf + rank * recvcount, sendbuf, recvcount);
  } else {
    int next = (rank + 1) % net_size;
    int fw_cnt = rank - root > 0 ? rank - root - 1 : (net_size + rank - root
        - 1) % net_size;

    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, next,
            GATHER_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gather: Unable to send gather message.\n");
    CHECK((tmp = malloc(sendcount * sizeof(*tmp))) != NULL || sendcount == 0,
        err, "gather: Out of memory.\n");

    for (i = 0; i < fw_cnt; ++i) {
      CHECK(MPI_Recv(tmp, sendcount, MPI_BYTE, prev,
              GATHER_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to receive message to forward.\n");
      CHECK(MPI_Send(tmp, sendcount, MPI_BYTE, next,
              GATHER_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "gather: Unable to forward message.\n");
    }
    free(tmp);
  }
  return 0;

  err: if (tmp != NULL) free(tmp);
  return 1;
}

int gatherv(int root, int rank, int net_size, void* sendbuf, int sendcount,
    void* recvbuf, int* recvcounts, int groupsize)
{
  MPI_Status status;
  int i;
  int prev = rank > 0 ? rank - 1 : net_size - 1;
  char* tmp = NULL;

  if (root == rank) {
    for (i = 1; i <= net_size - 1; ++i) {
      int next_index = (rank + net_size - i) % net_size;

      CHECK(MPI_Recv(recvbuf + get_offset(next_index, recvcounts), recvcounts[next_index], MPI_BYTE, prev,
              GATHERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gather: Unable to receive gather messages as root.\n");
    }
    memcpy(recvbuf + get_offset(rank, recvcounts), sendbuf, recvcounts[rank]);
  } else {
    int next = (rank + 1) % net_size;
    int fw_cnt = rank - root > 0 ? rank - root - 1 : (net_size + rank - root
        - 1) % net_size;

    /* Send first the size of the message, because only the root
     * and the current process know the size.*/
    if (next != root) {
      CHECK(MPI_Send(&sendcount, sizeof(sendcount), MPI_BYTE, next, GATHERV_TAG,
              MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "gatherv: Unable to forward first size.\n");
    }
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, next,
            GATHERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "gather: Unable to send gather message.\n");

    for (i = 0; i < fw_cnt; ++i) {
      int message_size;

      /* Receive the size of the message to forward. */
      CHECK(MPI_Recv(&message_size, sizeof(message_size), MPI_BYTE, prev,
              GATHERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to receive the size of the message to forward.");
      /* Reserve memory for the message to forward. */
      CHECK((tmp = realloc(tmp, message_size * sizeof(*tmp))) != NULL || message_size == 0,
          err, "gatherv: Out of memory!\n");
      /*Receive the message to forward.*/
      CHECK(MPI_Recv(tmp, message_size, MPI_BYTE, prev,
              GATHERV_TAG, MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "gatherv: Unable to receive message to forward.\n");
      /* Send the size of the message to the next node, except the root, because it knows
       * all the sizes.
       */
      if (next != root) {
        CHECK(MPI_Send(&message_size, sizeof(message_size), MPI_BYTE, next,
            GATHERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
            err, "gatherv: Unable to forward message size.\n");
      }
      /* Forward the message to the next node.*/
      CHECK(MPI_Send(tmp, message_size, MPI_BYTE, next,
              GATHERV_TAG, MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "gatherv: Unable to forward message.\n");
    }
    free(tmp);
  }
  return 0;

  err: if (tmp != NULL)
    free(tmp);
  return 1;
}

int broadcast(int root, int rank, int net_size, void* sendbuf, int sendcount)
{
  MPI_Status status;
  int next = (rank + 1) % net_size;

  if (root == rank) {
    CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, next, BCAST_TAG,
            MPI_COMM_WORLD) == MPI_SUCCESS,
        err, "broadcast: Error when initiating broadcast with MPI_Send as root.\n")
  } else {
    /* WARNING: Using modulo here is totally wrong because -5 % 3 = -2!!!!*/
    //int prev = (rank - 1) % net_size;
    int prev = rank > 0 ? rank - 1 : net_size - 1;

    /* WARNING: same warning as above :P*/
    //if (rank == ((root - 1) % net_size)) {
    if ((root > 0 && rank == root - 1) || (root == 0 && rank == net_size - 1)) {
      CHECK(MPI_Recv(sendbuf, sendcount, MPI_BYTE, prev, BCAST_TAG,
              MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "broadcast: Error when receiving message from root.\n");
    } else {
      CHECK(MPI_Recv(sendbuf, sendcount, MPI_BYTE, prev, BCAST_TAG,
              MPI_COMM_WORLD, &status) == MPI_SUCCESS,
          err, "broadcast: Error when receiving message from root.\n");
      CHECK(MPI_Send(sendbuf, sendcount, MPI_BYTE, next, BCAST_TAG,
              MPI_COMM_WORLD) == MPI_SUCCESS,
          err, "broadcast: Error when initiating broadcast with MPI_Send as root.\n")
    }
  }
  return 0;

  err: return 1;
}
