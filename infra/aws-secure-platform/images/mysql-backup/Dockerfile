FROM mysql:8.4

RUN microdnf install -y \
    awscli \
    tzdata \
  && microdnf clean all

COPY backup.sh /usr/local/bin/backup.sh

RUN chmod +x /usr/local/bin/backup.sh

ENTRYPOINT ["/usr/local/bin/backup.sh"]
