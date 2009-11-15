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

typedef struct RListNode* RList;

typedef struct reduce_list_node RListNode;

struct reduce_list_node
{
  MV val;
  RListNode* next;
};

typedef struct input_pair IPair;

struct input_pair
{
  IK key;
  IV val;
};

typedef struct MListNode* MList;

typedef struct map_list_node MListNode;

struct map_list_node
{
  MK key;
  MV val;
  MListNode* next;
};

typedef int (*input_reader_ptr)(const char* filename, int worker_count, IPair* result);
typedef int (*map_ptr)(IK input_key, IV input_value, MList* result);
typedef int (*reduce_ptr)(MK map_key, RList map_values, RList* result);

typedef struct map_reduce MapReduce;

struct map_reduce
{
  input_reader_ptr input_reader;
  map_ptr map;
  reduce_ptr reduce;
  int proc_count;
  int rank;
};

/*
 * Creates a new map/reduce application.
 * Requires the callbacks for reading the input, the map function and the reduce
 * function. Also, pointers to the application command line arguments must be supplied.
 * Returns the application on success, NULL otherwise.
 */
MapReduce* create_map_reduce_app(input_reader_ptr input_reader, map_ptr map,
    reduce_ptr reduce,int* argcp, char*** argvp);

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
