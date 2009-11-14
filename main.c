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

int main()
{
  PRINT("Pretending to call master function... \n");
  CHECK(master(NULL) == 0, master_err, "Master function failed!\n");
  PRINT("Pretending to call worker function... \n");
  CHECK(worker(NULL, NULL) == 0, worker_err, "Worker function failed!\n");
  exit(EXIT_SUCCESS);

worker_err:
master_err:
  exit(EXIT_FAILURE);
}

