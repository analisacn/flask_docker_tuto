FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV FLASK_APP app.py

RUN mkdir /code
RUN mkdir /docs

WORKDIR /code

ADD requirements.txt /code/

RUN pip install -r requirements.txt

ADD ./src /code/

ENTRYPOINT ["flask", "run"]

CMD ["--host=0.0.0.0"]
