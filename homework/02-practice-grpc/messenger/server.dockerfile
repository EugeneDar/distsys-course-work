FROM python:3.8-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir messenger
COPY __init__.py messenger/__init__.py
COPY server/ messenger/server/
COPY proto messenger/proto/

ENTRYPOINT ["python", "-m", "messenger.server.server"]
