/*
 * map_reduce.h
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#define MASTER_RANK 0

/*
 * Each application must define its own:
 *  INPUT_KEY_MAX_SIZE
 *  INPUT_VALUE_MAX_SIZE
 *  MAP_KEY_MAX_SIZE
 *  MAP_VALUE_MAX_SIZE
 */
#define APPLICATION 1
#if APPLICATION == 1

#define INPUT_KEY_MAX_SIZE 512
/* 1MB input chunks */
#define INPUT_VALUE_MAX_SIZE (1024 * 1024)
/* 512 bytes words */
#define MAP_KEY_MAX_SIZE 512
/* The map value is a counter. */
#define MAP_VALUE_MAX_SIZE sizeof(int)
#endif

#ifndef INPUT_KEY_MAX_SIZE
#error INPUT_KEY_MAX_SIZE missing
#endif

#ifndef INPUT_VALUE_MAX_SIZE
#error INPUT_VALUE_MAX_SIZE missing
#endif

#ifndef MAP_KEY_MAX_SIZE
#error MAP_KEY_MAX_SIZE missing
#endif

#ifndef MAP_VALUE_MAX_SIZE
#error MAP_VALUE_MAX_SIZE missing
#endif

typedef struct input_key IK;

struct input_key
{
  unsigned int size;
  char data[INPUT_KEY_MAX_SIZE];
};

typedef struct input_value IV;

struct input_value
{
  unsigned int size;
  char data[INPUT_VALUE_MAX_SIZE];
};

typedef struct map_key MK;

struct map_key
{
  unsigned int size;
  char data[MAP_KEY_MAX_SIZE];
};

typedef struct map_value MV;

struct map_value
{
  unsigned int size;
  char data[MAP_VALUE_MAX_SIZE];
};

typedef struct input_pair InputPair;

struct input_pair
{
  IK key;
  IV val;
};

typedef struct map_pair MapPair;

struct map_pair
{
  MK key;
  MV val;
};

/*
 * A mapping between map key and the worker that computed it.
 */
typedef struct map_key_worker_pair MapKeyWorkerPair;

struct map_key_worker_pair
{
  MK key;
  int worker;
};

/*
 * The master uses it to store the mappings between map keys and the workers
 * that computed them.
 */
typedef struct map_key_worker_pair_array MapKeyWorkerPairArray;

struct map_key_worker_pair_array
{
  MapKeyWorkerPair* array;
  unsigned int size;
};

/*
 * The input reader stores at most 'worker_count' tasks at the address pointed to by 'result'.
 * Returns the number of stored tasks on SUCCESS, -1 otherwise.
 */
typedef int (*input_reader_ptr)(const char* filename, int worker_count,
    InputPair* result);

/*
 * Takes an input_key and an input_value and returns an array of map (key, value) pairs.
 * Returns NULL on error.
 */
typedef MapPair* (*map_ptr)(InputPair* input_pair);

/*
 * Takes a map_key and a list of map_values and reduces the result.
 * Returns a list of map values on success, NULL otherwise.
 */
typedef MV* (*reduce_ptr)(MK* map_key, MV* map_values);

typedef struct map_reduce MapReduce;

struct map_reduce
{
  input_reader_ptr input_reader;

  map_ptr map;

  reduce_ptr reduce;

  int proc_count;
  int rank;
  const char* input_filename;

  MapKeyWorkerPairArray map_key_worker_mappings;
};

/*
 * Creates a new map/reduce application.
 * Requires the callbacks for reading the input, the map function and the reduce
 * function. Also, pointers to the application command line arguments must be supplied.
 * Returns the application on success, NULL otherwise.
 */
MapReduce* create_map_reduce_app(input_reader_ptr input_reader,
    map_ptr map, reduce_ptr reduce, int* argcp,
    char*** argvp, const char* input_filename);
/*
 * Destroys an existing map/reduce application.
 */
int destroy_map_reduce_app(MapReduce* app);

int is_master(MapReduce* app);

/*
 * Performs the master computation.
 */
int master(MapReduce* app);

/*
 * Performs the worker computation.
 */
int worker(MapReduce* app);

#endif
