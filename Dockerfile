FROM python:3.7.5
ENV PYTHONUNBUFFERED 1

WORKDIR /code

COPY requirements.txt .
RUN pip install pip==20.0.2
RUN pip install -r requirements.txt

COPY test_app/ /code/
COPY dqp/ ./dqp

CMD ["python", "manage.py", "test"]
