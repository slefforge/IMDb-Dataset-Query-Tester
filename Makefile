CC = gcc
CFLAGS = -Wall
LIBS = -lsqlite3

all: testQuery

testQuery: testQuery.c
	$(CC) $(CFLAGS) -o testQuery testQuery.c $(LIBS)

clean:
	rm -f testQuery