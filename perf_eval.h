/*
 * perf_eval.h
 *
 *  Created on: Dec 13, 2009
 *      Author: take
 */

#ifndef PERF_EVAL_H_
#define PERF_EVAL_H_

#include <stdio.h>

struct perf_eval
{
  FILE* log_file;
  int worker_id;
  int current_cnt;
  int collective_op_cnt;
};

typedef struct perf_eval PE;

PE* init_perf_eval(int worker_id);

void destroy_perf_eval(PE* pe);

void pe_start_bcast(PE* pe);

void pe_start_scatter(PE* pe);

void pe_start_scatterv(PE* pe);

void pe_start_gather(PE* pe);

void pe_start_gatherv(PE* pe);

int pe_send_message(PE* pe, int destination, int message_size);

int pe_recv_message(PE* pe, int source, int tag);

int pe_process_map_task(PE* pe, int map_key_cnt);

int pe_process_reduce_task(PE* pe, int reduce_key_cnt);

#endif /* PERF_EVAL_H_ */
