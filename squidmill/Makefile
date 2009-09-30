includedir=/usr/include
libdir=/usr/lib

squidmill: squidmill.o
	gcc squidmill.o squidmill_.o -Wl,-rpath,$(libdir)/gambit -lgambc \
	  -L$(libdir)/gambit -lgambc-sqlite3 -lgambc-signal -o squidmill

squidmill.o: squidmill.c
	gsc -:daq- -obj squidmill.c squidmill_.c

squidmill.c: squidmill.scm
	gsc -:daq- -link $(includedir)/gambit/libgambc-sqlite3.c \
	                 $(includedir)/gambit/libgambc-signal.c \
	                 squidmill.scm

clean:
	rm -f *.o *.c squidmill
