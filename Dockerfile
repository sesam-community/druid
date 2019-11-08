FROM python:3-alpine
MAINTAINER Sesam Community "support@sesam.io"
RUN apk update
RUN pip3 install --upgrade pip
RUN apk --update add build-base libffi-dev libressl-dev python-dev py-pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./service /service
EXPOSE 5000
CMD ["python3","-u","./service/datasource-service.py"]
