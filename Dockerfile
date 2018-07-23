FROM ubuntu

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y python3-pip python3 \
    libhdf4-alt-dev proj-bin libproj-dev libgdal-dev gdal-bin \
    python3-numpy python3-matplotlib python3-gdal python3-scipy

ADD requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
