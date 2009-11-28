/*
 * map_reduce.c
 *
 *  Created on: Nov 28, 2009
 *      Author: iris
 */

#include <stdlib.h>
#include <string.h>
#include "map_reduce_utils.h"

static void swap_kw(MapKeyWorkerPair* i, MapKeyWorkerPair* j)
{
  MapKeyWorkerPair temp;

  memcpy(&temp, i, sizeof(*i));
  memcpy(i, j, sizeof(*j));
  memcpy(j, &temp, sizeof(temp));
}

static int partition_kw_mappings(MapKeyWorkerPairArray* a, int l,
       int r, map_key_compare_ptr compare)
{
 int i = l-1;
 int j = r;
 MapKeyWorkerPair* v = &a->array[r];

 for(;;)
   {
        while(compare(&a->array[++i].key, &v->key) < 0);
        while(compare(&v->key, &a->array[--j].key) < 0) if (j == 1) break;
        if(i>=j) break;
        swap_kw(&a->array[i],&a->array[j]);
   }
   swap_kw(&a->array[i],&a->array[r]);
   return i;
}

static void sort_kw_mappings(MapKeyWorkerPairArray* a, unsigned int l,
       unsigned int r, map_key_compare_ptr compare)
{
  int i;
  if (r <= l) return;
  i = partition_kw_mappings(a, l, r, compare);
  sort_kw_mappings(a, l, i-1, compare);
  sort_kw_mappings(a, i+1, r, compare);
}

void sort_key_worker_mappings(MapKeyWorkerPairArray* a,
     map_key_compare_ptr compare)
{
  sort_kw_mappings(a, 0, a->size - 1, compare);
}

static void swap_kv(MapPair *i, MapPair *j)
{
  MapPair temp;

  memcpy(&temp, i, sizeof(*i));
  memcpy(i, j, sizeof(*j));
  memcpy(j, &temp, sizeof(temp));
}

static int partition_kv_mappings(MapKeyValuePairArray* a, int l,
       int r, map_key_compare_ptr compare)
{
 int i = l-1;
 int j = r;
 MapPair* v = &a->array[r];

 for(;;)
   {
        while(compare(&a->array[++i].key, &v->key) < 0);
        while(compare(&v->key, &a->array[--j].key) < 0) if (j == 1) break;
        if(i>=j) break;
        swap_kv(&a->array[i],&a->array[j]);
   }
   swap_kv(&a->array[i],&a->array[r]);
   return i;
}

static void sort_kv_mappings(MapKeyValuePairArray* a, unsigned int l,
       unsigned int r, map_key_compare_ptr compare)
{
  int i;
  if (r <= l) return;
  i = partition_kv_mappings(a, l, r, compare);
  sort_kv_mappings(a, l, i-1, compare);
  sort_kv_mappings(a, i+1, r, compare);
}

void sort_key_value_mappings(MapKeyValuePairArray* a,
     map_key_compare_ptr compare)
{
  sort_kv_mappings(a, 0, a->size - 1, compare);
}
