FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN apk --update add ca-certificates
RUN mkdir -p /etc/raintank
COPY build/eventtank.ini /etc/raintank/eventtank.ini

COPY build/eventtank /usr/bin/eventtank
COPY entrypoint.sh /usr/bin/

EXPOSE 6060

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["-config=/etc/raintank/eventtank.ini"]
