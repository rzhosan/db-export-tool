FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y g++ unixodbc-dev

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src .

ENTRYPOINT ["python", "main.py"]
