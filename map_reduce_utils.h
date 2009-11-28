/*
 * map_reduce.h
 *
 *  Created on: Nov 28, 2009
 *      Author: iris
 */

#ifndef MAP_REDUCE_UTILS_H
#define MAP_REDUCE_UTILS_H

#include "map_reduce.h"

void sort_key_worker_mappings(MapKeyWorkerPairArray* array, map_key_compare_ptr compare);

void sort_key_value_mappings(MapKeyValuePairArray* array, map_key_compare_ptr compare); 


#endif
