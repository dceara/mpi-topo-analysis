CC=mpicc
CFLAGS=-Wall

all: default

default: default_network.o network.o map_reduce.o main.o
	$(CC) -o $@ $(CFLAGS) $^

default_network.o: default_network.c network.h utils.h

network.o: network.c network.h utils.h

map_reduce.o: map_reduce.c map_reduce.h utils.h

main.o: main.c map_reduce.h utils.h

clean:
	rm -f default
	rm -f *.o
