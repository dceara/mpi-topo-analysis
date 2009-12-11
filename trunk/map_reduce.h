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
 *  struct input_key
 *  struct input_value
 *  struct map_key
 *  struct map_value
 */
#define WORD_COUNTING 1
#define DISTRIBUTED_GREP 2

#if APPLICATION == WORD_COUNTING
/* 1MB input chunks */
//#define INPUT_VALUE_MAX_SIZE (1024 * 1024)
#define INPUT_VALUE_MAX_SIZE 32
/* 512 bytes words */
#define MAP_KEY_MAX_SIZE 512
/* The map value is a counter. */
#define MAP_VALUE_MAX_SIZE sizeof(int)

struct input_key
{
};

struct input_value
{
  char data[INPUT_VALUE_MAX_SIZE];
};

struct map_key
{
  char word[MAP_KEY_MAX_SIZE];
};

struct map_value
{
  unsigned int counter;
};
#endif

#if APPLICATION == DISTRIBUTED_GREP
/* 1MB input chunks */
//#define INPUT_VALUE_MAX_SIZE (1024 * 1024)
#define INPUT_VALUE_MAX_SIZE 1024
/* 512 bytes lines */
#define MAP_KEY_MAX_SIZE 512
struct input_key
{
};

struct input_value
{
  char data[INPUT_VALUE_MAX_SIZE];
};

struct map_key
{
  char line[MAP_KEY_MAX_SIZE];
};

struct map_value
{
  unsigned int found;
};
#endif

typedef struct input_key IK;

typedef struct input_value IV;

typedef struct map_key MK;

typedef struct map_value MV;

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

typedef struct map_key_value_pair_array MapKeyValuePairArray;

struct map_key_value_pair_array
{
  MapPair* array;
  unsigned int size;
};

typedef int (*map_key_compare_ptr)(const MK* first, const MK* second);

/*
 * The input reader stores at most 'worker_count' tasks at the address pointed to by 'result'.
 * Returns the number of stored tasks on SUCCESS, -1 otherwise.
 */
typedef int (*input_reader_ptr)(const char* filename, int worker_count,
    InputPair* result);

/*
 * Takes an input_key and an input_value, returns an array of map (key, value) pairs and
 * stores the size of the result in 'results_cnt'
 * Returns NULL on error.
 */
typedef MapPair* (*map_ptr)(InputPair* input_pair, int* results_cnt);

/*
 * Takes a map_key and a list of all map_key/value pairs and reduces the requested key.
 * Returns a list of map values on success, NULL otherwise.
 */
typedef MV
* (*reduce_ptr)(int worker, MK* key_to_reduce, MapPair* all_values, int total_cnt);

typedef struct map_reduce MapReduce;

struct map_reduce
{
  input_reader_ptr input_reader;

  map_ptr map;

  reduce_ptr reduce;

  map_key_compare_ptr map_key_compare;

  int proc_count;
  int rank;
  const char* input_filename;

  MapKeyWorkerPairArray map_key_worker_mappings;
  MapKeyValuePairArray map_key_value_mappings;
  MapKeyValuePairArray reduce_key_value_mappings;
};

/*
 * Creates a new map/reduce application.
 * Requires the callbacks for reading the input, the map function and the reduce
 * function. Also, pointers to the application command line arguments must be supplied.
 * Returns the application on success, NULL otherwise.
 */
MapReduce* create_map_reduce_app(input_reader_ptr input_reader, map_ptr map,
    map_key_compare_ptr map_key_compare, reduce_ptr reduce, int* argcp,
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
