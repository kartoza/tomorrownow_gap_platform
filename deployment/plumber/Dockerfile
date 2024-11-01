FROM rstudio/plumber:v1.2.0 AS prod

# install python 3.12
RUN apt-get update -y && apt-get upgrade -y

RUN apt install software-properties-common -y

RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update -y && apt-get upgrade -y

RUN apt install python3.12 -y
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
RUN update-alternatives --set python3 /usr/bin/python3.12
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | PIP_BREAK_SYSTEM_PACKAGES=1 python3

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y --no-install-recommends \
    gcc gettext cron build-essential \
    spatialite-bin libsqlite3-mod-spatialite \
    python3.12-dev python3-gdal python3-psycopg2 python3-ldap libpython3.12-dev \
    python3-pil python3-lxml python3-pylibmc \
    uwsgi uwsgi-plugin-python3 \
    libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev

# install R packages
RUN install2.r --error tidyverse tidygam mgcv ggpubr classInt zoo

# Install pip packages
ADD deployment/docker/requirements.txt /requirements.txt
RUN apt-get remove -y python3-pil
RUN PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install setuptools Pillow
# Fix uwsgi build failure missing cc1
ARG CPUCOUNT=1
RUN PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install -r /requirements.txt

# add django project
ADD django_project /home/web/django_project

# create directories
RUN mkdir -p /home/web/plumber_data/

# add entrypoint.sh
ADD django_project/plumber_entrypoint.sh /home/web/plumber_entrypoint.sh

EXPOSE 8181

WORKDIR /home/web/django_project
ENTRYPOINT [ "/home/web/plumber_entrypoint.sh" ]
