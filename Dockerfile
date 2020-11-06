# ------------------------------
# Build stage
# ------------------------------
FROM ubuntu:groovy as build-stage

# runtime dependencies
RUN apt update && apt install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    netcat \
    software-properties-common \
    gnupg gnupg-agent \
    && rm -rf /var/lib/apt/lists/*

# install java
RUN apt update && apt install -y --no-install-recommends \
    openjdk-11-jre \
    && rm -rf /var/lib/apt/lists/*

# python 3.8.6 is intalled by default on ubuntu 20.04, just upgrade
RUN apt -y upgrade \
    && add-apt-repository -y universe \
    && apt install -y --no-install-recommends python3-pip

# ------------------------------
# Prod stage
# ------------------------------
FROM build-stage as prod-stage

# create the appropriate directories
ENV APP_HOME=/usr/src/app
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
RUN python3 -m pip install --upgrade pip pipenv
COPY ./Pipfile $APP_HOME/Pipfile
RUN sed -i 's/psycopg2/psycopg2-binary/g' ./Pipfile
RUN pipenv install

# copy project
COPY . $APP_HOME

# run entrypoint.sh
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]

# Upgrade DB and run app
CMD bash -c "pipenv run gunicorn --access-logfile '-' --bind 0.0.0.0:4041 run:app"
