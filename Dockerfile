FROM python:3.9-bullseye

WORKDIR /app

COPY requirements-1.txt .

RUN pip install --upgrade pip==22.1.1

RUN pip install --no-cache-dir -r requirements-1.txt

COPY . /app

WORKDIR /app

ENV PATH /app:$PATH

USER root

EXPOSE 4000

RUN chmod -R 777 .

CMD [ "flask", "run", "--port", "4000", "--host", "0.0.0.0" ]