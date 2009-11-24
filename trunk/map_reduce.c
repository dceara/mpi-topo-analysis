/*
 * map_reduce.c
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#include <stdlib.h>
#include <string.h>
#include "map_reduce.h"
#include "utils.h"
#include "network.h"

#define MAP_TASK 1
#define FINISHED_MAP 2

static char SEND_MAP_TASK[1] =
{ MAP_TASK };
static char FINISHED_MAP_TASK[1] =
{ FINISHED_MAP };

MapReduce* create_map_reduce_app(input_reader_ptr input_reader, map_ptr map,
    map_key_compare_ptr map_key_compare, reduce_ptr reduce, int* argcp,
    char*** argvp, const char* input_filename)
{
  MapReduce* app;

  CHECK(input_reader != NULL, param_err,
      "create_map_reduce_app: Invalid input_reader parameter!\n");
  CHECK(map != NULL, param_err,
      "create_map_reduce_app: Invalid map parameter!\n");
  CHECK(map_key_compare != NULL, param_err,
      "create_map_reduce_app: Invalid map_key_compare parameter!\n");
  CHECK(reduce != NULL, param_err,
      "create_map_reduce_app: Invalid reduce parameter!\n");
  CHECK(init_network(argcp, argvp) == 0, init_err,
      "create_map_reduce_app: Unable to initialize network layer!\n");

  CHECK((app = malloc(sizeof(*app))) != NULL, alloc_err,
      "create_map_reduce_app: Out of memory when creating map/reduce application!\n");
  app->input_reader = input_reader;
  app->map = map;
  app->map_key_compare = map_key_compare;
  app->reduce = reduce;
  CHECK(get_network_size(&app->proc_count) == 0, size_err,
      "create_map_reduce_app: Unable to get network size!\n");
  CHECK(get_network_rank(&app->rank) == 0, rank_err,
      "create_map_reduce_app: Unable to get network rank!\n");
  app->input_filename = input_filename;

  app->map_key_worker_mappings.array = NULL;
  app->map_key_worker_mappings.size = 0;

  app->map_key_value_mappings.array = NULL;
  app->map_key_value_mappings.size = 0;

  app->reduce_key_value_mappings.array = NULL;
  app->reduce_key_value_mappings.size = 0;

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
  if (app->map_key_value_mappings.size != 0)
    free(app->map_key_value_mappings.array);
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

    CHECK(available_cnt != -1,
        input_err, "distribute_map_tasks: Unable to read input tasks.\n");
    CHECK(available_cnt < app->proc_count,
        input_err, "distribute_map_tasks: input_reader returned too many tasks.\n");

    DBG_PRINT("rank: %d: Called input_reader. Received %d tasks.\n", app->rank, available_cnt);

    if (available_cnt == 0) {
      DBG_PRINT("rank: %d: No more map tasks to send.\n", app->rank);
      break;
    }

    /* Initialize sizes for each worker. sizes[i] = 1 if worker gets task, 0 otherwise.*/
    sizes[0] = 0;
    for (i = 0; i < available_cnt; ++i)
      sizes[i + 1] = sizeof(*tasks);
    for (i = available_cnt + 1; i < app->proc_count; ++i)
      sizes[i] = 0;

    /* Inform the workers that we have Map tasks for them. */
    DBG_PRINT("rank: %d: Inform the workers that we have tasks for them.\n", app->rank);
    CHECK(broadcast(app->rank, SEND_MAP_TASK, sizeof(SEND_MAP_TASK)) == 0,
        bcast_err, "distribute_map_tasks: Unable to broadcast input tasks signal.\n");

    /* Announce workers whether they have available tasks or not. */
    DBG_PRINT("rank: %d: Send the sizes of the tasks to the workers.\n", app->rank);
    CHECK(scatter(app->rank, sizes, sizeof(*sizes), buf, sizeof(*sizes)) == 0,
        scatter_err, "distribute_map_tasks: Unable to scatter tasks sizes.\n");

    /* Send tasks to the workers. */
    DBG_PRINT("rank: %d: Send the actual tasks to the workers.\n", app->rank);
    CHECK(scatterv(app->rank, tasks, sizes, NULL, 0, app->proc_count) == 0,
        scatter_err, "distribute_map_tasks: Unable to scatter tasks.\n");

    /* Receive number of result keys from each worker. */
    DBG_PRINT("rank: %d: Receive the number of result keys from each worker.\n", app->rank);
    CHECK(gather(app->rank, buf, sizeof(*sizes), sizes, sizeof(*sizes)) == 0,
        gather_err, "distribute_map_tasks: Unable to gather number of result keys.\n");

    for (i = 0; i < app->proc_count; ++i) {
      total_map_mappings_cnt += sizes[i] / sizeof(MapKeyWorkerPair);
      DBG_PRINT("rank: %d: worker %d will send %d keys.\n", app->rank,
          i, sizes[i] / sizeof(MapKeyWorkerPair));
    }

    /* Increase the storing space in the global array for the new map keys. */
    CHECK((app->map_key_worker_mappings.array = realloc(app->map_key_worker_mappings.array,
                (old_mappings_cnt + total_map_mappings_cnt) * sizeof(MapKeyWorkerPair))) != NULL,
        realloc_err, "distribute_map_tasks: Out of memory!\n");
    app->map_key_worker_mappings.size += total_map_mappings_cnt;

    /* Receive the map keys from the workers and store them in the global array. */
    DBG_PRINT("rank: %d: receiving the map keys from the workers and storing" \
        " them in the global array\n", app->rank);
    CHECK(gatherv(app->rank, NULL, 0,
            app->map_key_worker_mappings.array + old_mappings_cnt, sizes, app->proc_count) == 0,
        gatherv_err, "distribute_map_tasks: Unable to receive key/worker pairs.\n");

    /* If we have no more tasks, finish distributing them. */
    if (available_cnt < app->proc_count - 1) {
      DBG_PRINT("rank: %d: No more map tasks to send.\n", app->rank);
      break;
    }
  }

  /* No more tasks. Announce workers.*/
  DBG_PRINT("rank: %d: Announcing workers that we have no more map tasks.\n", app->rank);
  CHECK(broadcast(app->rank, FINISHED_MAP_TASK, 1) == 0, bcast_err,
      "master: Unable to broadcast FINISHED_MAP_TASK to workers.\n");

  free(sizes);
  free(tasks);
  return 0;

  gatherv_err: realloc_err: gather_err: scatter_err: bcast_err: input_err: free(
      sizes);
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
  CHECK(broadcast(app->rank, &app->map_key_worker_mappings.size,
          sizeof(app->map_key_worker_mappings.size)) == 0,
      bcast_err, "broadcast_key_worker_mappings: Unable to broadcast mappings count.\n")

  /* Broadcast the actual mappings. */
  CHECK(broadcast(app->rank, app->map_key_worker_mappings.array,
          app->map_key_worker_mappings.size * sizeof(MapKeyWorkerPair)) == 0,
      bcast_err, "broadcast_key_worker_mappings: Unable to broadcast mappings.\n");
  return 0;

  bcast_err: return 1;
}

int workers_scatter(MapReduce* app)
{
  int worker;
  int size;

  for (worker = 1; worker < app->proc_count; ++worker) {
    CHECK(scatter(worker, NULL, 0, &size, sizeof(size)) == 0,
        scatter_err, "workers_scatter: Unable to get size.\n");
    CHECK(scatterv(worker, NULL, NULL, NULL, 0, app->proc_count) == 0,
        scatterv_err, "workers_scatter: Unable to call scatterv.\n");
  }

  return 0;

  scatter_err: scatterv_err: return 1;
}

int master(MapReduce* app)
{

  CHECK(app != NULL, param_err, "master: Invalid application parameter!\n");
  CHECK(app->proc_count > 1, param_err,
      "master: Unable to start map/reduce without any workers!\n");

  DBG_PRINT("rank: %d: Distributing map tasks.\n", app->rank);
  CHECK(distribute_map_tasks(app) == 0, distrib_err, "master: Unable to distribute map tasks.\n")

  DBG_PRINT("rank: %d: Broadcasting key/worker mappings.\n", app->rank);
  CHECK(broadcast_key_worker_mappings(app) == 0, bcast_kw_err,
      "master: Unable to broadcast key/worker mappings.\n");

  DBG_PRINT("rank: %d: Master simulating scatter.\n", app->rank);
  CHECK(workers_scatter(app) == 0, scatter_err, "master: Unable to scatter with workers.\n");
  // TODO: maybe wait for results here. If not, then the master can finish.
  return 0;

  scatter_err: distrib_err: bcast_kw_err: param_err: return 1;
}

static inline int perform_map_task(MapReduce* app)
{
  int task_available = 0;
  int task_size;
  InputPair task;
  MapPair* map_results;
  int map_results_cnt = 0;
  MapKeyWorkerPair* key_worker_array = NULL;

  /* Check if the master has a task available for the worker. */
  DBG_PRINT("rank: %d: Checking if the master has a task available for me.\n", app->rank);
  CHECK(scatter(MASTER_RANK, NULL, 0, &task_available, sizeof(task_available)) == 0,
      avail_err, "perform_map_task: Unable to receive task_available from master.\n");

  task_size = task_available ? sizeof(task) : 0;

  /* Get the task from the master if available. */
  DBG_PRINT("rank: %d: Getting the task from the master. Task size: %d\n", app->rank, task_size);
  CHECK(scatterv(MASTER_RANK, NULL, NULL, &task, task_size, app->proc_count) == 0,
      rec_task_err, "perform_map_task: Unable to receive task from master.\n");

  if (task_available) {
    int i;
    int old_mappings_size = app->map_key_value_mappings.size;

    /* Perform the map operation. */
    CHECK((map_results = app->map(&task, &map_results_cnt)) != NULL,
        map_err, "perform_map_task: Unable to execute map operation.\n");

    /* Send the number of result keys to the master. */
    CHECK(gather(MASTER_RANK, &map_results_cnt, sizeof(map_results_cnt), NULL, 0) == 0,
        gather_err, "perform_map_task: Unable to send number of result keys to the master.\n");

    CHECK((app->map_key_value_mappings.array =
            realloc(app->map_key_value_mappings.array,
                (old_mappings_size + map_results_cnt) * sizeof(MapPair)))!=NULL,
        mkv_alloc_err, "perform_map_task: Out of memory!\n");
    app->map_key_value_mappings.size += map_results_cnt;

    for (i = 0; i < map_results_cnt; ++i)
      memcpy(&app->map_key_value_mappings.array[old_mappings_size + i],
          &map_results[i], sizeof(*map_results));

    /* Create key_worker_array with the newly computed keys. */
    CHECK((key_worker_array = malloc(map_results_cnt * sizeof(*key_worker_array))) != NULL,
        alloc_err, "perform_map_task: Out of memory!\n");
    for (i = 0; i < map_results_cnt; ++i) {
      memcpy(&key_worker_array[i].key, &map_results[i].key,
          sizeof(key_worker_array[i].key));
      key_worker_array[i].worker = app->rank;
    }

    /* Send the key_worker array to the master. */
    CHECK(gatherv(MASTER_RANK, key_worker_array, map_results_cnt * sizeof(*key_worker_array), NULL, 0, app->proc_count) == 0,
        gatherv_err, "perform_map_task: Unable to send key_worker array to the master.\n");
    free(key_worker_array);
    free(map_results);
  } else {
    DBG_PRINT("rank: %d: Sending size 0 as a result to mock map task.\n", app->rank);
    CHECK(gather(MASTER_RANK, &map_results_cnt, sizeof(map_results_cnt), NULL, 0) == 0,
        gather_err, "perform_map_task: Unable to send 0 as number of result keys to the master.\n");

    DBG_PRINT("rank: %d: Sending empty result to mock map task.\n", app->rank);
    CHECK(gatherv(MASTER_RANK, NULL, 0, NULL, 0, app->proc_count) == 0,
        gatherv_err, "perform_map_task: Unable to send empty key_worker array to the master.\n")
  }

  return 0;

  gatherv_err: free(key_worker_array);
  alloc_err: mkv_alloc_err: free(map_results);
  gather_err: map_err: rec_task_err: avail_err: return 1;
}

static inline int receive_map_tasks(MapReduce* app)
{
  char signal[1];

  for (;;) {
    int finished = 0;

    DBG_PRINT("rank: %d: Receiving broadcast signal from master.\n", app->rank);
    CHECK(broadcast(MASTER_RANK, signal, sizeof(signal)) == 0,
        signal_err, "receive_map_tasks: Unable to receive signal from master.\n");
    switch (signal[0])
    {
    case FINISHED_MAP:
      DBG_PRINT("rank: %d: Finished performing map tasks.\n", app->rank);
      finished = 1;
      break;
    case MAP_TASK:
      DBG_PRINT("rank: %d: Received a map task from the master.\n", app->rank);
      CHECK(perform_map_task(app) == 0,
          map_err, "receive_map_tasks: Unable to perform map task.\n");
      break;
    default:
      ERR("Worker %d: Received unknown signal from master: %c\n", app->rank, signal[0]);
      break;
    }
    if (finished)
      break;
  }
  return 0;

  map_err: signal_err: return 1;
}

static inline int receive_key_worker_mappings(MapReduce* app)
{
  CHECK(broadcast(MASTER_RANK, &app->map_key_worker_mappings.size,
          sizeof(app->map_key_worker_mappings.size)) == 0,
      err, "receive_key_worker_mappings: Unable to receive key/worker mappings size from master.\n");
  CHECK((app->map_key_worker_mappings.array =
          malloc(app->map_key_worker_mappings.size * sizeof(MapKeyWorkerPair))) != NULL,
      err, "receive_key_worker_mappings: Out of memory!\n");
  CHECK(broadcast(MASTER_RANK, app->map_key_worker_mappings.array,
          app->map_key_worker_mappings.size * sizeof(MapKeyWorkerPair)) == 0,
      err, "receive_key_worker_mappings: Unable to receive map key/worker mappings.\n");
  return 0;

  err: return 1;
}

static inline int get_worker_for_key(MapReduce* app, MK* key,
    int keys_for_worker)
{
  int i;
  int current_unique_key_index = 0;
  MapKeyWorkerPair* p = NULL;

  for (i = 0; i < app->map_key_worker_mappings.size; ++i) {
    if (p == NULL || app->map_key_compare(&p->key,
        &app->map_key_worker_mappings.array[i].key) != 0) {
      ++current_unique_key_index;
      p = &app->map_key_worker_mappings.array[i];
    }
    if (app->map_key_compare(key, &app->map_key_worker_mappings.array[i].key)
        == 0)
      break;
  }

  if (i >= app->map_key_worker_mappings.size)
    BUG("BUG: Worker %d: get_worker_for_key. Missing key.\n", app->rank);

  return (current_unique_key_index - 1) / keys_for_worker + 1;
}

static inline int send_reduce_data(MapReduce* app)
{
  int unique_key_cnt = 0;
  int i;
  MapKeyWorkerPair* p = NULL;
  int worker;
  int keys_for_worker;

  NOT_IMPLEMENTED("send_reduce_data: sort the map_key/values array");

  for (i = 0; i < app->map_key_worker_mappings.size; ++i) {
    if (p == NULL || app->map_key_compare(&p->key,
        &app->map_key_worker_mappings.array[i].key) != 0) {
      ++unique_key_cnt;
      p = &app->map_key_worker_mappings.array[i];
    }
  }

  keys_for_worker = (unique_key_cnt - 1) / (app->proc_count - 1);

  for (worker = 1; worker < app->proc_count; ++worker) {
    int size;
    int old_reduce_size = app->reduce_key_value_mappings.size;

    if (worker == app->rank) {
      /* It's my turn to send the data. */
      int* sizes;

      CHECK((sizes = calloc(app->proc_count, sizeof(*sizes))) != NULL,
          sizes_alloc_err, "send_reduce_data: Out of memory!\n");

      /* Send sizes to other workers. */
      for (i = 0; i < app->map_key_value_mappings.size; ++i) {
        MK* key = &app->map_key_value_mappings.array[i].key;
        sizes[get_worker_for_key(app, key, keys_for_worker)] += sizeof(MapPair);
      }

      // TODO: We have a memory leak here: if the check fails then 'sizes' is never freed.
      CHECK(scatter(app->rank, sizes, sizeof(*sizes), &size, sizeof(size)) == 0,
          scatter_err, "send_reduce_data: Worker %d: Unable to scatter sizes to other workers",
          app->rank);

      size = size / sizeof(MapPair);

      // TODO: We have a memory leak here: if the check fails then 'sizes' is never freed.
      CHECK((app->reduce_key_value_mappings.array =
              realloc(app->reduce_key_value_mappings.array,
                  (old_reduce_size + size) * sizeof(MapPair))) != NULL,
          realloc_err, "send_reduce_data: Out of memory!\n");
      app->reduce_key_value_mappings.size += size;

      // TODO: We have a memory leak here: if the check fails then 'sizes' is never freed.
      CHECK(scatterv(app->rank, app->map_key_value_mappings.array,
              sizes, app->reduce_key_value_mappings.array + old_reduce_size,
              size, app->proc_count) == 0,
          scatterv_err, "send_reduce_data: Worker %d: Unable to scatter map values to other workers.\n",
          app->rank);

      free(sizes);
    } else {
      CHECK(scatter(worker, NULL, 0, &size, sizeof(size)) == 0,
          scatter_err, "send_reduce_data: Worker %d: Unable to receive size from worker %d.\n",
          app->rank, worker);

      size = size / sizeof(MapPair);

      CHECK((app->reduce_key_value_mappings.array =
              realloc(app->reduce_key_value_mappings.array,
                  (old_reduce_size + size) * sizeof(MapPair))) != NULL,
          realloc_err, "send_reduce_data: Out of memory!\n");
      app->reduce_key_value_mappings.size += size;

      CHECK(scatterv(worker, NULL, NULL, app->reduce_key_value_mappings.array + old_reduce_size, size, app->proc_count) == 0,
          scatterv_err, "send_reduce_data: Worker %d: Unable to receive values from worker %d.\n",
          app->rank, worker);
    }
  }

  return 0;

  scatterv_err: scatter_err: realloc_err: sizes_alloc_err: return 1;
}

static inline int perform_reduce(MapReduce* app)
{
  int i, j;
  int* reduced;

  CHECK((reduced = calloc(app->reduce_key_value_mappings.size, sizeof(*reduced))) != NULL,
      alloc_err, "perform_reduce: Out of memory!\n");

  for (i = 0; i < app->reduce_key_value_mappings.size; ++i) {
    MK* first;

    if (reduced[i])
      continue;

    reduced[i] = 1;
    first = &app->reduce_key_value_mappings.array[i].key;
    for (j = i + 1; j < app->reduce_key_value_mappings.size; ++j) {
      MK* second = &app->reduce_key_value_mappings.array[j].key;
      if (app->map_key_compare(first, second) == 0)
        reduced[j] = 1;
    }
    // TODO: do something with the reduce result.. or not :)
    (void) app->reduce(first, app->reduce_key_value_mappings.array);
  }

  free(reduced);
  return 0;

  alloc_err: return 1;
}

int worker(MapReduce* app)
{
  CHECK(app != NULL, err, "worker: Invalid application parameter!\n");

  DBG_PRINT("rank: %d: Receiving map tasks from master.\n", app->rank);
  CHECK(receive_map_tasks(app) == 0, err, "worker: Unable to receive map operations.\n");

  DBG_PRINT("rank: %d: Receiving key/worker mappings from master.\n", app->rank);
  CHECK(receive_key_worker_mappings(app) == 0, err, "worker: Unable to receive key worker mappings.\n");

  DBG_PRINT("rank: %d: Sending reduce data to other workers.\n", app->rank);
  CHECK(send_reduce_data(app) == 0, err, "worker: Unable to send reduce data.\n");

  DBG_PRINT("rank: %d: Performing reduce locally.\n", app->rank);
  CHECK(perform_reduce(app) == 0, err, "worker: Unable to perform reduce.\n");
  return 0;

  err: return 1;
}

