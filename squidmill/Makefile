prefix=/usr
includedir=$(prefix)/include
libdir=$(prefix)/lib
sbindir=$(prefix)/sbin
sysconfdir=/etc
initdir=$(sysconfdir)/rc.d/init.d
unitdir=/lib/systemd/system

squidmill: squidmill.o
	gcc squidmill.o squidmill_.o -Wl,-rpath,$(libdir)/gambit -lgambit \
	  -L$(libdir)/gambit -lgambit-sqlite3 -lgambit-signal -lgambit-dsock -o squidmill

squidmill.o: squidmill.c
	gsc -:daq- -obj -cc-options "$(CFLAGS)" squidmill.c squidmill_.c

squidmill.c: squidmill.scm
	gsc -:daq- -link $(includedir)/gambit/libgambit-sqlite3.c \
	                 $(includedir)/gambit/libgambit-signal.c \
	                 $(includedir)/gambit/libgambit-dsock.c \
	                 squidmill.scm

install:
	install -p -m0755 -D squidmill $(sbindir)/squidmill
	install -p -m0755 -D squidmill-service $(initdir)/squidmill
	install -p -m0644 -D squidmill.service $(unitdir)/squidmill.service
	install -p -m0644 -D squidmill.conf $(sysconfdir)/sysconfig/squidmill

check: squidmill
	./squidmill-test ./squidmill squidmill-test.d

clean:
	rm -f *.o *.c squidmill
