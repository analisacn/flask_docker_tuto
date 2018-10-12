FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV FLASK_APP app.py

RUN mkdir /code

WORKDIR /code

ADD requirements.txt /code/

RUN pip install -r requirements.txt

ADD ./flask /code/

ENTRYPOINT ["flask", "run"]

CMD ["--host=0.0.0.0"]