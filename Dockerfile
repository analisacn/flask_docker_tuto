FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV FLASK_APP app.py

RUN mkdir /src
RUN mkdir /docs

WORKDIR /src

ADD requirements.txt /src/

RUN pip install -r requirements.txt

ADD ./src /src/

ENTRYPOINT ["flask", "run"]

CMD ["--host=0.0.0.0"]
