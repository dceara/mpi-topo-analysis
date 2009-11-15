/*
 * map_reduce.h
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#define MASTER_RANK 0

typedef struct generic_key_value IK;

typedef struct generic_key_value IV;

typedef struct generic_key_value MK;

typedef struct generic_key_value MV;

struct generic_key_value
{
  unsigned int size;
  void* data;
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

typedef struct map_pair_array MapArray;

struct map_pair_array
{
  MapPair* map_pair_array;
  unsigned int size;
};

typedef struct map_value_array MapValueArray;

struct map_value_array
{
  MV* map_val_array;
  unsigned int size;
};

/*
 * The input reader should return a new pair (input_key, input_value) at subsequent calls.
 * Returns 0 on a successful read, 1 otherwise.
 */
typedef int (*input_reader_ptr)(const char* filename, int worker_count, InputPair* result);

/*
 * Returns the size of the serialized key.
 */
typedef int (*input_key_size_ptr)(IK* key);

/*
 * Returns the size of the serialized value.
 */
typedef int (*input_value_size_ptr)(IV* val);

/*
 * Serializes the input pair into ptr.
 */
typedef void (*input_serialize_ptr)(InputPair* p, char* ptr);

/*
 * Takes an input_key and an input_value and returns a list of map (key, value) pairs.
 * Returns 0 on success, 1 otherwise.
 */
typedef int (*map_ptr)(InputPair* input_pair, MapArray* result);

/*
 * Takes a map_key and a list of map_values and reduces the result.
 * Returns 0 on success, 1 otherwise.
 */
typedef int (*reduce_ptr)(MK* map_key, MapArray* map_values, MapArray* result);

typedef struct map_reduce MapReduce;

struct map_reduce
{
  input_reader_ptr input_reader;
  input_key_size_ptr input_key_size;
  input_value_size_ptr input_value_size;
  input_serialize_ptr input_serialize;

  map_ptr map;
  reduce_ptr reduce;
  int proc_count;
  int rank;
  const char* input_filename;
};

/*
 * Creates a new map/reduce application.
 * Requires the callbacks for reading the input, the map function and the reduce
 * function. Also, pointers to the application command line arguments must be supplied.
 * Returns the application on success, NULL otherwise.
 */
MapReduce* create_map_reduce_app(input_reader_ptr input_reader,
    input_key_size_ptr input_key_size, input_value_size_ptr input_value_size,
    input_serialize_ptr input_serialize, map_ptr map, reduce_ptr reduce,
    int* argcp, char*** argvp, const char* input_filename);

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
