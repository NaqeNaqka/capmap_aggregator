FROM python:3.12 

RUN mkdir /app

COPY ./src /app

COPY requirements.txt /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python", "main.py"]