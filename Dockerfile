FROM python:3.12.1-slim-bullseye
LABEL Maintainer="fermihdi"

WORKDIR /usr/app/src
COPY *.py ./
COPY requirements.txt ./
RUN mkdir /data && \
    pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./genrate_flows.py", "-x", "-o /data/"]