/*
 * map_reduce.c
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#include <stdlib.h>
#include "map_reduce.h"
#include "utils.h"
#include "network.h"

static char SEND_MAP_TASK[1] =
{ 1 };
static char FINISHED_MAP_TASK[1] =
{ 2 };

MapReduce* create_map_reduce_app(input_reader_ptr input_reader, map_ptr map,
    reduce_ptr reduce, int* argcp, char*** argvp, const char* input_filename)
{
  MapReduce* app;

  CHECK(input_reader != NULL, param_err,
      "create_map_reduce_app: Invalid input_reader parameter!\n");
  CHECK(map != NULL, param_err,
      "create_map_reduce_app: Invalid map parameter!\n");
  CHECK(reduce != NULL, param_err,
      "create_map_reduce_app: Invalid reduce parameter!\n");
  CHECK(init_network(argcp, argvp) == 0, init_err,
      "create_map_reduce_app: Unable to initialize network layer!\n");

  CHECK((app = malloc(sizeof(*app))) != NULL, alloc_err,
      "create_map_reduce_app: Out of memory when creating map/reduce application!\n");
  app->input_reader = input_reader;
  app->map = map;
  app->reduce = reduce;
  CHECK(get_network_size(&app->proc_count) == 0, size_err,
      "create_map_reduce_app: Unable to get network size!\n");
  CHECK(get_network_rank(&app->rank) == 0, rank_err,
      "create_map_reduce_app: Unable to get network rank!\n");
  app->input_filename = input_filename;

  app->map_key_worker_mappings.array = NULL;
  app->map_key_worker_mappings.size = 0;

  return app;

  rank_err: size_err: free(app);
  alloc_err: init_err: param_err: return NULL;
}

int destroy_map_reduce_app(MapReduce* app)
{
  if (app == NULL)
    return 1;
  if (app->map_key_worker_mappings.size != 0)
    free(app->map_key_worker_mappings.array);
  free(app);
  finalize_network();
  return 0;
}

int is_master(MapReduce* app)
{
  return app->rank == MASTER_RANK;
}

static inline int distribute_map_tasks(MapReduce* app)
{
  InputPair* tasks;
  int* sizes;
  int buf[1];

  CHECK((tasks = malloc((app->proc_count - 1) * sizeof(*tasks))) != NULL,
      tasks_alloc_err, "distribute_map_tasks: Out of memory!\n");
  CHECK((sizes = calloc(app->proc_count, sizeof(*sizes))) != NULL,
      sizes_alloc_err, "distribute_map_tasks: Out of memory!\n");

  for (;;) {
    int i;
    int old_mappings_cnt = app->map_key_worker_mappings.size;
    int total_map_mappings_cnt = 0;
    int available_cnt = app->input_reader(app->input_filename, app->proc_count
        - 1, tasks);

    CHECK(available_cnt != -1, input_err, "distribute_map_tasks: Unable to read input tasks.\n");

    /* Initialize sizes for each worker. sizes[i] = 1 if worker gets task, 0 otherwise.*/
    sizes[0] = 0;
    for (i = 0; i < available_cnt; ++i)
      sizes[i + 1] = sizeof(*tasks);
    for (i = available_cnt; i < app->proc_count; ++i)
      sizes[i] = 0;

    /* Inform the workers that we have Map tasks for them. */
    CHECK(broadcast(app->rank, SEND_MAP_TASK, sizeof(SEND_MAP_TASK)) == 0,
        bcast_err, "distribute_map_tasks: Unable to broadcast input tasks signal.\n");

    /* Announce workers whether they have available tasks or not. */
    CHECK(scatter(app->rank, sizes, sizeof(*sizes), buf, sizeof(*sizes)) == 0,
        scatter_err, "distribute_map_tasks: Unable to scatter tasks sizes.\n");

    /* Send tasks to the workers. */
    CHECK(scatterv(app->rank, tasks, sizes, NULL, 0, app->proc_count),
        scatter_err, "distribute_map_tasks: Unable to scatter tasks.\n");

    /* Receive number of result keys from each worker. */
    CHECK(gather(app->rank, buf, sizeof(*sizes), sizes, sizeof(*sizes)) == 0,
        gather_err, "distribute_map_tasks: Unable to gather number of result keys.\n");

    for (i = 0; i < app->proc_count; ++i)
      total_map_mappings_cnt += sizes[i] / sizeof(MapKeyWorkerPair);

    /* Increase the storing space in the global array for the new map keys. */
    CHECK((app->map_key_worker_mappings.array = realloc(app->map_key_worker_mappings.array,
                (old_mappings_cnt + total_map_mappings_cnt) * sizeof(MapKeyWorkerPair))) != NULL,
        realloc_err, "distribute_map_tasks: Out of memory!\n");
    app->map_key_worker_mappings.size += total_map_mappings_cnt;

    /* Receive the map keys from the workers and store them in the global array. */
    CHECK(gatherv(app->rank, NULL, 0,
            app->map_key_worker_mappings.array + old_mappings_cnt, sizes, app->proc_count) == 0,
        realloc_err, "distribute_map_tasks: Unable to receive key/worker pairs.\n");

    /* If we have no more tasks, finish distributing them. */
    if (available_cnt < app->proc_count - 1)
      break;
  }

  /* No more tasks. Announce workers.*/
  CHECK(broadcast(app->rank, FINISHED_MAP_TASK, 1) == 0, bcast_err,
      "master: Unable to broadcast FINISHED_MAP_TASK to workers.\n");

  free(sizes);
  free(tasks);
  return 0;

  realloc_err: gather_err: scatter_err: bcast_err: input_err: free(sizes);
  sizes_alloc_err: free(tasks);
  tasks_alloc_err: return 1;
}

/*
 * Returns 0 on success, -1 otherwise.
 */
static inline int broadcast_key_worker_mappings(MapReduce* app)
{
  NOT_IMPLEMENTED("broadcast_key_worker_mappings: sort the map_key/worker array");
  /* Broadcast the number of mappings.*/
  CHECK(broadcast(app->rank, &app->map_key_worker_mappings.size, sizeof(app->map_key_worker_mappings.size)) == 0,
      bcast_err, "broadcast_key_worker_mappings: Unable to broadcast mappings count.\n")

  /* Broadcast the actual mappings. */
  CHECK(broadcast(app->rank, app->map_key_worker_mappings.array,
          app->map_key_worker_mappings.size * sizeof(MapKeyWorkerPair)) == 0,
      bcast_err, "broadcast_key_worker_mappings: Unable to broadcast mappings.\n");
  return 0;

  bcast_err: return 1;
}

int master(MapReduce* app)
{

  CHECK(app != NULL, param_err, "master: Invalid application parameter!\n");
  CHECK(app->proc_count > 1, param_err,
      "master: Unable to start map/reduce without any workers!\n");

  CHECK(distribute_map_tasks(app) == 0, distrib_err, "master: Unable to distribute map tasks.\n")
  CHECK(broadcast_key_worker_mappings(app) == 0, bcast_kw_err,
      "master: Unable to broadcast key/worker mappings.\n");

  // TODO: maybe wait for results here. If not, then the master can finish.
  return 0;

  distrib_err: bcast_kw_err: param_err: return 1;
}

int worker(MapReduce* app)
{
  CHECK(app != NULL, param_err, "worker: Invalid application parameter!\n");
  NOT_IMPLEMENTED("worker");
  return 0;

  param_err: return 1;
}

