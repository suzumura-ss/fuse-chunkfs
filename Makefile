# DEBUG_OPT=-g -O0
CFLAGS=-Wall -O3

chunkfs: chunkfs.cpp chunkfs.h Makefile
	gcc ${CFLAGS} ${DEBUG_OPT} -o chunkfs chunkfs.cpp `pkg-config fuse --cflags --libs` -lstdc++
