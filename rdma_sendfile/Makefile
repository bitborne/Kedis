CC = gcc
CFLAGS = -Wall -O2
LDFLAGS = -lrdmacm -libverbs -lpthread

TARGETS = sf_server sf_client rdma_server rdma_client

all: $(TARGETS)

sf_server: sf_server.c
	$(CC) $(CFLAGS) -o $@ $<

sf_client: sf_client.c
	$(CC) $(CFLAGS) -o $@ $<

rdma_server: rdma_server.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

rdma_client: rdma_client.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS)

.PHONY: all clean
