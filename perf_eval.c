/*
 * perf_eval.c
 *
 *  Created on: Dec 13, 2009
 *      Author: take
 */

#include "perf_eval.h"
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

static const char BCAST_ID[] = "BCAST";
static const char SCATTER_ID[] = "SCATTER";
static const char SCATTERV_ID[] = "SCATTERV";
static const char GATHER_ID[] = "GATHER";
static const char GATHERV_ID[] = "GATHERV";

static const int SEND_TAG = 512;

PE* init_perf_eval(int worker_id)
{
  char filename[10];
  PE* pe = NULL;

  sprintf(filename, "%d.log", worker_id);
  CHECK((pe = malloc(sizeof(*pe))) != NULL, err,
      "init_perf_eval: Out of memory!\n");
  pe->log_file = NULL;
  CHECK((pe->log_file = fopen(filename, "w")) != NULL, err,
      "init_perf_eval: Unable to open log file %s.\n", filename);
  pe->collective_op_cnt = 0;
  pe->current_cnt = 0;
  pe->worker_id = worker_id;
  return pe;

  err: if (pe != NULL)
    free(pe);
  return NULL;
}

void destroy_perf_eval(PE* pe)
{
  assert(pe != NULL);
  fclose(pe->log_file);
  free(pe);
}

#define PERF_EVAL
#ifdef PERF_EVAL

struct worker_count
{
  int worker;
  int cnt;
};

void pe_start_bcast(PE* pe)
{
  assert(pe != NULL);
  //fprintf(pe->log_file, "%s %d\n", BCAST_ID, pe->collective_op_cnt);
  //++pe->collective_op_cnt;
}

void pe_start_scatter(PE* pe)
{
  assert(pe != NULL);
  //fprintf(pe->log_file, "%s %d\n", SCATTER_ID, pe->collective_op_cnt);
  //++pe->collective_op_cnt;
}

void pe_start_scatterv(PE* pe)
{
  assert(pe != NULL);
  //fprintf(pe->log_file, "%s %d\n", SCATTERV_ID, pe->collective_op_cnt);
  //++pe->collective_op_cnt;
}

void pe_start_gather(PE* pe)
{
  assert(pe != NULL);
  //fprintf(pe->log_file, "%s %d\n", GATHER_ID, pe->collective_op_cnt);
  //++pe->collective_op_cnt;
}

void pe_start_gatherv(PE* pe)
{
  assert(pe != NULL);
  //fprintf(pe->log_file, "%s %d\n", GATHERV_ID, pe->collective_op_cnt);
  //++pe->collective_op_cnt;
}

int pe_send_message(PE* pe, int dest, int message_size)
{
  struct worker_count wc;

  assert(pe != NULL);
#warning check return codes.
  wc.worker = pe->worker_id;
  wc.cnt = pe->current_cnt;
  DBG_PRINT("rank %d: sending wc.worker = %d wc.cnt = %d to dest = %d\n", pe->worker_id, wc.worker, wc.cnt, dest);
  MPI_Send(&wc, sizeof(struct worker_count), MPI_BYTE, dest, SEND_TAG,
      MPI_COMM_WORLD);
  fprintf(pe->log_file, "SEND_MESSAGE:id=%d:dest=%d:size=%d\n",
      pe->current_cnt, dest, message_size);
  ++pe->current_cnt;
  return 0;
}

int pe_recv_message(PE* pe, int source, int tag)
{
  int waiting = 0;
  int flag = 0;
  MPI_Status status;
  struct worker_count wc;

  assert(pe != NULL);
#warning check return codes.
  MPI_Iprobe(source, SEND_TAG, MPI_COMM_WORLD, &flag, &status);
  if (!flag)
    waiting = 1;
  DBG_PRINT("rank %d: receiving wc from source = %d\n", pe->worker_id, source);
  MPI_Recv(&wc, sizeof(struct worker_count), MPI_BYTE, source, SEND_TAG,
      MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  DBG_PRINT("rank %d: received wc from source = %d\n", pe->worker_id, source);
  if (waiting) {
    fprintf(pe->log_file,
        "RECV_MESSAGE_DELAYED:id=%d:source=%d:source_cnt=%d\n",
        pe->current_cnt, wc.worker, wc.cnt);
  } else {
    MPI_Iprobe(source, tag, MPI_COMM_WORLD, &flag, &status);
    if (!flag) {
      fprintf(pe->log_file,
          "RECV_MESSAGE_DELAYED:id=%d:source=%d:source_cnt=%d\n",
          pe->current_cnt, wc.worker, wc.cnt);
    } else {
      fprintf(pe->log_file, "RECV_MESSAGE:id=%d\n", pe->current_cnt);
    }
  }
  ++pe->current_cnt;
  return 0;
}

int pe_process_map_task(PE* pe, int map_key_cnt)
{
  fprintf(pe->log_file, "PROCESS_MAP:id=%d:key_cnt=%d\n", pe->current_cnt, map_key_cnt);
  ++ pe->current_cnt;
  return 0;
}

int pe_process_reduce_task(PE* pe, int reduce_key_cnt)
{
  fprintf(pe->log_file, "PROCESS_REDUCE:id=%d:key_cnt=%d\n", pe->current_cnt, reduce_key_cnt);
  ++ pe->current_cnt;
  return 0;
}
#else

void pe_start_bcast(PE* pe)
{
}

void pe_start_scatter(PE* pe)
{
}

void pe_start_scatterv(PE* pe)
{
}

void pe_start_gather(PE* pe)
{
}

void pe_start_gatherv(PE* pe)
{
}

int pe_send_message(PE* pe, int destination, int message_size)
{
  return 0;
}

int pe_recv_message(PE* pe, int source, int tag, int message_size)
{
  return 0;
}

int pe_process_map_task(PE* pe, int map_key_cnt)
{
  return 0;
}

int pe_process_reduce_task(PE* pe, int reduce_key_cnt)
{
  return 0;
}

#endif
