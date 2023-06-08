FROM python:3.11-slim-buster

ENV CELERY_LOG_LEVEL=$CELERY_LOG_LEVEL

COPY requirements.txt /tmp/

RUN apt-get update && apt-get install -y libpq-dev gcc python-dev supervisor nginx openssl curl && \
  mkdir -p /etc/nginx/ssl/ && \
  openssl req \
            -x509 \
            -subj "/C=US/ST=Massachusetts/L=Cambridge/O=Dis" \
            -nodes \
            -days 365 \
            -newkey rsa:2048 \
            -keyout /etc/nginx/ssl/nginx.key \
            -out /etc/nginx/ssl/nginx.cert && \
  chmod -R 755 /etc/nginx/ssl/ && \
  pip install --upgrade pip && \
  pip install --upgrade --force-reinstall -r /tmp/requirements.txt -i https://pypi.org/simple/ --extra-index-url https://test.pypi.org/simple/

RUN useradd --create-home etdadm
WORKDIR /home/etdadm

COPY --chown=etdadm ./ .

# Update permissions for the etdadm user and group
COPY change_id.sh /root/change_id.sh
RUN chmod 755 /root/change_id.sh && \
  /root/change_id.sh -u 55031 -g 55031

USER etdadm

CMD ["celery", "-A", "tasks.tasks", "worker", "--loglevel=info", "--queues", "etd_ingested_into_drs"]
