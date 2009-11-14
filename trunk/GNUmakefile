CC=mpicc
CFLAGS=-Wall

all: main

main: map_reduce.o main.o

map_reduce.o: map_reduce.c map_reduce.h utils.h

main.o: main.c map_reduce.h utils.h


