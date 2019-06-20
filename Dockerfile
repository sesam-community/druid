FROM python:3
MAINTAINER Graham Moore "graham.moore@sesam.io"
RUN pip install --upgrade pip
COPY ./service /service
WORKDIR /service
RUN pip install -r requirements.txt
EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["datasource-service.py"]
