/*
 * map_reduce.h
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#ifndef __MAP_REDUCE_H
#define __MAP_REDUCE_H

typedef struct generic_key_value IK;

typedef struct generic_key_value IV;

typedef struct generic_key_value MK;

typedef struct generic_key_value MV;

struct generic_key_value {
  unsigned int size;
  void* data;
};

typedef struct RListNode* RList;

typedef struct reduce_list_node RListNode;

struct reduce_list_node {
  MV val;
  RListNode* next;
};

typedef struct input_pair IPair;

struct input_pair {
  IK key;
  IV val;
};

typedef struct MListNode* MList;

typedef struct map_list_node MListNode;

struct map_list_node {
  MK key;
  MV val;
  MListNode* next;
};

typedef int (*input_reader_ptr)(const char* filename, int worker_count, IPair* result);

typedef int (*map_ptr)(IK input_key, IV input_value, MList* result);

typedef int (*reduce_ptr)(MK map_key, RList map_values, RList* result);

int master(input_reader_ptr reader);

int worker(map_ptr map, reduce_ptr reduce);

#endif
