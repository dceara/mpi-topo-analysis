/*
 * utils.h
 *
 *  Created on: Nov 14, 2009
 *      Author: take
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <stdio.h>
#include <assert.h>

#define NOT_IMPLEMENTED(message) \
  do { \
    printf("Functionality not implemented yet: %s\n", message); \
    assert(0); \
  } while (0)

#define DEBUG
#ifdef DEBUG
#define DBG_PRINT(format, ...) \
  do { \
    fprintf(stdout, format, ##__VA_ARGS__); \
    fflush(stdout); \
  } while(0)
#else
#define DBG_PRINT(format, ...)
#endif

#define PRINT(format, ...) \
  do { \
    fprintf(stdout, format, ##__VA_ARGS__); \
    fflush(stdout); \
  } while(0)

#define ERR(format, ...) \
  do { \
    fprintf(stderr, format, ##__VA_ARGS__); \
    fflush(stderr); \
  } while(0)

#define CHECK(condition, lbl, format, ...) \
  if (!(condition)) { \
    ERR(format, ##__VA_ARGS__); \
    goto lbl; \
  }

#define BUG(format, ...) \
  do { \
    ERR(format, ##__VA_ARGS__); \
    assert(0); \
  } while(0)


#endif /* UTILS_H_ */
