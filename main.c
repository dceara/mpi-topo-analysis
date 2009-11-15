/*
 * main.c
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#include <stdio.h>
#include <stdlib.h>
#include "map_reduce.h"
#include "utils.h"

/* TODO: Change mock input reader function! */
int input_reader(const char* filename, int worker_count, IPair* result)
{
  return 0;
}

/* TODO: Change mock input reader function! */
int map(IK input_key, IV input_value, MList* result)
{
  return 0;
}

/* TODO: Change mock input reader function! */
int reduce(MK map_key, RList map_values, RList* result)
{
  return 0;
}


int main(int argc, char** argv)
{
  MapReduce* app;

  PRINT("Creating map reduce application... \n");
  CHECK((app = create_map_reduce_app(input_reader, map, reduce, &argc, &argv)) != NULL, app_err,
      "Unable to create map reduce application\n");

  if (is_master(app)) {
    PRINT("Calling master function... \n");
    CHECK(master(app) == 0, master_err, "Master function failed!\n");
  } else {
    PRINT("Calling worker function... \n");
    CHECK(worker(app) == 0, worker_err, "Worker function failed!\n");
  }

  destroy_map_reduce_app(app);
  exit(EXIT_SUCCESS);


worker_err:
master_err:
  destroy_map_reduce_app(app);
app_err:
  exit(EXIT_FAILURE);
}

