/*
 * map_reduce.c
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#include <mpi.h>

#include <stdlib.h>
#include <strings.h>
#include "map_reduce.h"
#include "utils.h"
#include "network.h"

static char SEND_MAP_TASK[1] =
{ 1 };
static char FINISHED_MAP_TASK[1] =
{ 2 };

MapReduce* create_map_reduce_app(input_reader_ptr input_reader,
    input_key_size_ptr input_key_size, input_value_size_ptr input_value_size,
    input_serialize_ptr input_serialize, map_ptr map, reduce_ptr reduce,
    int* argcp, char*** argvp, const char* input_filename)
{
  MapReduce* app;

  CHECK(input_reader != NULL, param_err,
      "create_map_reduce_app: Invalid input_reader parameter!\n");
  CHECK(input_key_size != NULL, param_err,
      "create_map_reduce_app: Invalid input_key_size parameter!\n");
  CHECK(input_value_size != NULL, param_err,
      "create_map_reduce_app: Invalid input_value_size parameter!\n");
  CHECK(input_serialize != NULL, param_err,
      "create_map_reduce_app: Invalid input_serialize parameter!\n");
  CHECK(map != NULL, param_err,
      "create_map_reduce_app: Invalid map parameter!\n");
  CHECK(reduce != NULL, param_err,
      "create_map_reduce_app: Invalid reduce parameter!\n");
  CHECK(init_network(argcp, argvp) == 0, init_err,
      "create_map_reduce_app: Unable to initialize network layer!\n");

  CHECK((app = malloc(sizeof(*app))) != NULL, alloc_err,
      "create_map_reduce_app: Out of memory when creating map/reduce application!\n");
  app->input_reader = input_reader;
  app->input_key_size = input_key_size;
  app->input_value_size = input_value_size;
  app->input_serialize = input_serialize;
  app->map = map;
  app->reduce = reduce;
  CHECK(get_network_size(&app->proc_count) == 0, size_err,
      "create_map_reduce_app: Unable to get network size!\n");
  CHECK(get_network_rank(&app->rank) == 0, rank_err,
      "create_map_reduce_app: Unable to get network rank!\n");
  app->input_filename = input_filename;

  return app;

  rank_err: size_err: free(app);
  alloc_err: init_err: param_err: return NULL;
}

int destroy_map_reduce_app(MapReduce* app)
{
  if (app == NULL)
    return 1;
  free(app);
  finalize_network();
  return 0;
}

int is_master(MapReduce* app)
{
  return app->rank == MASTER_RANK;
}

static inline int serialize_tasks(MapReduce* app, InputPair* input_pair_arr,
    int available_tasks, char** tasks, int** sizes)
{
  int i;
  int total_size = 0;
  int current_pos = 0;

  /*
   * Reserve space for the first task which is empty and goes to the master.
   */
  CHECK((*sizes = calloc(app->proc_count, sizeof(**sizes))) != NULL, sizes_alloc_err,
      "serialize_tasks: Out of memory!\n");

  (*sizes)[0] = 0;
  for (i = 0; i < available_tasks; ++i) {
    InputPair* p = &input_pair_arr[i];
    int key_size = app->input_key_size(&p->key);
    int value_size = app->input_value_size(&p->val);

    total_size += key_size + value_size;
    *sizes[i + 1] = key_size + value_size;
  }
  CHECK((*tasks = calloc(total_size, sizeof(**tasks))) != NULL, tasks_alloc_err,
      "serialize_tasks: Out of memory!\n");
  for (i = 0; i < available_tasks; ++i) {
    InputPair* p = &input_pair_arr[i];
    char* ptr = (*tasks) + current_pos;

    app->input_serialize(p, ptr);
    current_pos += (*sizes)[i];
  }
  return 0;

  tasks_alloc_err: free(*sizes);
  sizes_alloc_err: return 1;
}

/*
 * Returns -1 on error, 0 if there are no more tasks, 1 otherwise.
 */
static inline int distribute_map_tasks(MapReduce* app)
{
  InputPair* input_pair_arr;
  int available_tasks = 0;
  int finished = 0;
  char* tasks;
  int* sizes;
  int i;
  int buf[1];

  CHECK((input_pair_arr = calloc(app->proc_count - 1, sizeof(*input_pair_arr))) != NULL,
      alloc_err, "distribute_map_tasks: Out of memory.\n");

  for (i = 0; i < app->proc_count - 1; ++i) {
    if (app->input_reader(app->input_filename, app->proc_count - 1,
        &input_pair_arr[i]) != 0) {
      finished = 1;
      break;
    }
    ++available_tasks;
  }
  if (finished && available_tasks == 0) {
    free(input_pair_arr);
    return 0;
  }

  /* Inform the workers that we have Map tasks for them. */
  CHECK(broadcast(app->rank, SEND_MAP_TASK, sizeof(SEND_MAP_TASK)) == 0, bcast_err,
      "distribute_map_tasks: Unable to broadcast input tasks signal");

  CHECK(serialize_tasks(app, input_pair_arr, available_tasks, &tasks, &sizes) == 0, serialize_err,
      "distribute_map_tasks: Unable to serialize input tasks");

  /* First scatter the sizes to all workers in order for them to prepare the buffers. */
  CHECK(scatter(app->rank, sizes, sizeof(int), buf, sizeof(int)) == 0, scatter_err,
      "distribute_map_tasks: Unable to scatter task sizes.\n");
  /* Scatter the actual tasks to the workers. */
  CHECK(scatterv(app->rank, tasks, sizes, NULL, 0, app->proc_count) == 0, scatter_err,
      "distribute_map_tasks: Unable to scatter input tasks.\n");
  free(tasks);
  free(sizes);
  free(input_pair_arr);
  if (finished)
    return 0;
  return 1;

  scatter_err: free(tasks);
  free(sizes);
  bcast_err: serialize_err: free(input_pair_arr);
  alloc_err: return -1;
}

int master(MapReduce* app)
{

  CHECK(app != NULL, param_err, "master: Invalid application parameter!\n");
  CHECK(app->proc_count > 1, param_err, "master: Unable to start map/reduce without any workers!\n");

  /*
   * TODO: We make the assumption that we have at least app->proc_count available tasks!
   */
  for (;;) {
    int result = distribute_map_tasks(app);
    if (result == -1) {
      /* An error has occurred. */
      return 1;
    } else if (result == 0) {
      /* No more tasks. Announce workers.*/
      CHECK(broadcast(app->rank, FINISHED_MAP_TASK, 1) == 0, bcast_err,
          "master: Unable to broadcast FINISHED_MAP_TASK to workers.\n");
      break;
    }
    //TODO: wait for response for map operation!
  }

  NOT_IMPLEMENTED("master");
  return 0;

  bcast_err: param_err: return 1;
}

int worker(MapReduce* app)
{
  CHECK(app != NULL, param_err, "worker: Invalid application parameter!\n");
  NOT_IMPLEMENTED("worker");
  return 0;

  param_err: return 1;
}

