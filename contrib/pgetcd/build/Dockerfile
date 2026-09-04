ARG ALPINE_VERSION=3.23
FROM alpine:${ALPINE_VERSION}

ADD etcd /usr/local/bin/
ADD etcdctl /usr/local/bin/
ADD etcdutl /usr/local/bin/
ADD tools/benchmark /usr/local/bin/
RUN mkdir -p /var/etcd/ && mkdir -p /var/lib/etcd/

WORKDIR /var/etcd/
WORKDIR /var/lib/etcd/

EXPOSE 2379 2380 6060

# Define default command.
CMD ["/usr/local/bin/etcd"]
