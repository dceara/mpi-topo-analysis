CC=mpicc
#word counting
APP=WORD_COUNTING
#distributed grep
#APP=DISTRIBUTED_GREP

#debugging CFLAGS
DEBUG=-DDEBUG
#release
#DEBUG=-UDEBUG

CFLAGS=-Wall -DAPPLICATION=$(APP) $(DEBUG)

all: default ring test

test: test.o ring_network.o

test.o: test.c

default: default_network.o network.o map_reduce.o map_reduce_utils.o main.o
	$(CC) -o $@ $(CFLAGS) $^
	
ring: ring_network.o network.o map_reduce.o map_reduce_utils.o main.o
	$(CC) -o $@ $(CFLAGS) $^

ring_network.o: ring_network.c network.h utils.h

default_network.o: default_network.c network.h utils.h

network.o: network.c network.h utils.h

map_reduce_utils.o: map_reduce_utils.c map_reduce.h map_reduce_utils.h

map_reduce.o: map_reduce.c map_reduce.h utils.h

main.o: main.c map_reduce.h utils.h

clean:
	rm -f ring
	rm -f default
	rm -f *.o
