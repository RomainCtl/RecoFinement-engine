# pull official base image
FROM python:3.8.1-slim-buster

# create the appropriate directories
ENV APP_HOME=/usr/src/app
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y netcat

# install dependencies
RUN pip install --upgrade pip pipenv
COPY ./Pipfile $APP_HOME/Pipfile
RUN sed -i 's/psycopg2/psycopg2-binary/g' ./Pipfile
RUN pipenv install

# copy project
COPY . $APP_HOME

# run entrypoint.sh
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]

# Upgrade DB and run app
CMD bash -c "pipenv run gunicorn --access-logfile '-' --bind 0.0.0.0:4041 run:app"
