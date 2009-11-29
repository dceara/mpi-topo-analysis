/*
 * main.c
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "map_reduce.h"
#include "utils.h"

#if APPLICATION == WORD_COUNT
int input_reader(const char* filename, int worker_count, InputPair* result)
{
  static int fd = -1;
  int i;

  if (fd == -1) {
    CHECK((fd = open(filename, O_RDONLY)) != -1, open_err,
        "input_reader: Unable to open input file %s\n", filename);
  }
  for (i = 0; i < worker_count; ++i) {
    int read_cnt = read(fd, &result[i].val.data, INPUT_VALUE_MAX_SIZE - 1);
    CHECK(read_cnt != -1, read_err,
        "input_reader: Error when reading from file %s.\n", filename);
    result[i].val.size = read_cnt;
    result[i].val.data[read_cnt] = 0;
    if (read_cnt == 0) {
      close(fd);
      return i;
    }
    if (read_cnt < INPUT_VALUE_MAX_SIZE - 1) {
      ++i;
      break;
    }
  }
  return i;

  read_err: close(fd);
  open_err: return -1;
}

MapPair* map(InputPair* input_pair, int* results_cnt)
{
  /* BE CAREFUL HERE because after the strtok is called the input is changed!!!*/
  const char delim[] = " \t\r\n";
  char* token;
  int current_max_size = 16;
  MapPair* results;

  *results_cnt = 0;
  CHECK((results = calloc(current_max_size, sizeof(*results))) != NULL,
      alloc_err, "map: Out of memory!\n");

  token = strtok(input_pair->val.data, delim);
  while (token != NULL) {
    int existing = 0;
    int i;

    for (i = 0; i < *results_cnt; ++i) {
      if (strcmp(results[i].key.data, token) == 0) {
        existing = 1;
        break;
      }
    }
    if (existing) {
      int cnt = *((int*) (&results[i].val.data));

      ++cnt;
      memcpy(results[i].val.data, &cnt, sizeof(cnt));
    } else {
      int val = 1;
      int key_size;
      int str_len;

      if (*results_cnt == current_max_size) {
        current_max_size *= 2;
        CHECK((results = realloc(results, current_max_size * sizeof(*results))) != NULL,
            alloc_err, "map: Out of memory!\n");
      }
      *results_cnt = *results_cnt + 1;
      // TODO check overflows
      str_len = strlen(token);
      key_size = str_len <= MAP_KEY_MAX_SIZE - 1 ? str_len : MAP_KEY_MAX_SIZE - 1;
      strncpy(results[*results_cnt - 1].key.data, token, key_size);
      results[*results_cnt - 1].key.data[str_len] = 0;
      results[i].key.size = key_size;
      memcpy(results[*results_cnt - 1].val.data, &val, sizeof(val));
      results[*results_cnt - 1].val.size = sizeof(val);
    }
    token = strtok(NULL, delim);
  }
  return results;

  alloc_err: *results_cnt = -1;
  return NULL;
}

int map_key_compare(const MK* first, const MK* second)
{
  int len;

  if (first == NULL && second == NULL)
    return 0;
  if (first == NULL && second != NULL)
    return -1;
  if (first != NULL && second == NULL)
    return 1;

  len = first->size > second->size ? first->size : second->size;
  len = len <= MAP_KEY_MAX_SIZE - 1 ? len : MAP_KEY_MAX_SIZE - 1;
  return strncmp(first->data, second->data, len);
}

MV* reduce(MK* key_to_reduce, MapPair* all_values, int total_cnt)
{
  int i;

  int total = 0;
  for (i = 0; i < total_cnt; ++i) {
    if (map_key_compare(key_to_reduce, &all_values[i].key) == 0) {
      total += *((int*) (all_values[i].val.data));
    }
  }
  PRINT("Reduce result: for key: %s -> value %d.\n", key_to_reduce->data, total);
  return 0;
}
#endif

int main(int argc, char** argv)
{
  MapReduce* app;

  PRINT("Creating map reduce application... \n");
  CHECK((app = create_map_reduce_app(input_reader, map, map_key_compare, reduce,
              &argc, &argv, "input.txt")) != NULL, app_err,
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

