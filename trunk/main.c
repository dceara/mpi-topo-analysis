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
int input_reader(const char* filename, int worker_count, InputPair* result)
{
  PRINT("input_reader called.\n");
  return 1;
}

/* TODO: Change mock input reader function! */
MapPair* map(InputPair* input_pair, int* results_cnt)
{
  return NULL;
}

/* TODO: Change mock input reader function! */
MV* reduce(MK* key_to_reduce, MapPair* all_values)
{
  return 0;
}

int map_key_compare(const MK* first, const MK* second)
{
  if (first == NULL && second == NULL)
    return 0;
  if (first == NULL && second != NULL)
    return -1;
  if (first != NULL && second == NULL)
    return 1;

  NOT_IMPLEMENTED("map_key_compare");
  return 0;
}

int main(int argc, char** argv)
{
  MapReduce* app;

  PRINT("Creating map reduce application... \n");
  CHECK((app = create_map_reduce_app(input_reader, map, map_key_compare, reduce,
              &argc, &argv, "test.txt")) != NULL, app_err,
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

  worker_err: master_err: destroy_map_reduce_app(app);
  app_err: exit(EXIT_FAILURE);
}

