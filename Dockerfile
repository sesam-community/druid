FROM python:3
MAINTAINER Graham Moore "graham.moore@sesam.io"
RUN pip install --upgrade pip
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY ./service /service
WORKDIR /service
EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["datasource-service.py"]
