FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt setup.py setup.cfg setup.py versioneer.py ./
COPY redsql ./redsql
COPY docker/etc/redsql /etc/redsql

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

CMD ["redsql", "--config", "/etc/redsql/config.yml"]