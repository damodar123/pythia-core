
CC=gcc -g

all: wait.start wait.stop
	mkfifo start stop

wait.start: wait.start.c

wait.stop: wait.stop.c

clean:
	rm -f wait.start wait.stop
	rm -f start stop
