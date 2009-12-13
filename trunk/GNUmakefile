CC=mpicc
#word counting
#APP=WORD_COUNTING
#distributed grep
APP=DISTRIBUTED_GREP

#debugging CFLAGS
#DEBUG=-DDEBUG
#release
DEBUG=-UDEBUG

#performance evaluation
PERF=-DPERF_EVAL
#no performance evaluation
#PERF=-UPERF_EVAL

CFLAGS=-Wall -lm -DAPPLICATION=$(APP) $(DEBUG) $(PERF)

all: default ring grid test

test: test.o perf_eval.o grid_network.o

test.o: test.c

default: perf_eval.o default_network.o network.o map_reduce.o map_reduce_utils.o main.o
	$(CC) -o $@ $(CFLAGS) $^
		
ring: perf_eval.o ring_network.o network.o map_reduce.o map_reduce_utils.o main.o
	$(CC) -o $@ $(CFLAGS) $^
	
grid: perf_eval.o grid_network.o network.o map_reduce.o map_reduce_utils.o main.o
	$(CC) -o $@ $(CFLAGS) $^

ring_network.o: ring_network.c network.h utils.h perf_eval.h

grid_network.o: grid_network.c network.h utils.h perf_eval.h

default_network.o: default_network.c network.h utils.h perf_eval.h

network.o: network.c network.h utils.h

perf_eval.o: perf_eval.c perf_eval.h

map_reduce_utils.o: map_reduce_utils.c map_reduce.h map_reduce_utils.h

map_reduce.o: map_reduce.c map_reduce.h network.h utils.h perf_eval.h

main.o: main.c map_reduce.h utils.h

clean: clean_grid clean_ring clean_default

clean_obj:
	rm -f *.o

clean_grid: clean_obj
	rm -f grid
	
clean_ring: clean_obj
	rm -f ring

clean_default: clean_obj
	rm -f default
