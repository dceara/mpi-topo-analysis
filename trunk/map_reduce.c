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

MapReduce* create_map_reduce_app(input_reader_ptr input_reader, map_ptr map,
    reduce_ptr reduce, int* argcp, char*** argvp)
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

  return app;

rank_err:
size_err:
  free(app);
alloc_err:
init_err:
param_err:
  return NULL;
}

int destroy_map_reduce_app(MapReduce* app)
{
  if (app == NULL)
    return 1;
  free(app);
  finalize_network();
  return 0;
}

inline int is_master(MapReduce* app)
{
  return app->rank == MASTER_RANK;
}

int master(MapReduce* app)
{
  CHECK(app != NULL, param_err, "master: Invalid application parameter!\n");
  NOT_IMPLEMENTED("master");
  return 0;
param_err:
  return 1;
}

int worker(MapReduce* app)
{
  CHECK(app != NULL, param_err, "worker: Invalid application parameter!\n");
  NOT_IMPLEMENTED("worker");
  return 0;
param_err:
  return 1;
}

