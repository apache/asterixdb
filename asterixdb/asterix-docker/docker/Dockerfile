#/*
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM centos
MAINTAINER AsterixDB Team

RUN echo 'LANG="en_US.UTF-8"' > /etc/sysconfig/i18n ;echo 'ZONE="America/Los_Angeles"' > /etc/sysconfig/clock ;cp -a /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN echo "include_only=.us" >> /etc/yum/pluginconf.d/fastestmirror.conf
RUN yum -y update
RUN yum install -y unzip java-11-openjdk-headless python-setuptools wget curl
RUN easy_install supervisor
RUN mkdir /asterixdb
COPY asterix-server*.zip .
RUN unzip asterix-server*.zip -d /asterixdb/
RUN mv /asterixdb/apache*/* /asterixdb/
COPY supervisord.conf /etc/supervisord.conf
COPY twu.adm /asterixdb/twu.adm
COPY twm.adm /asterixdb/twm.adm
COPY fbu.adm /asterixdb/fbu.adm
COPY fbm.adm /asterixdb/fbm.adm

WORKDIR /asterixdb/bin
ENV JAVA_HOME /usr/lib/jvm/jre
ENV JAVA_OPTS -Xmx1536m
EXPOSE 19001 19002 8888 19003 50031

ENTRYPOINT /bin/bash -c '/usr/bin/supervisord'


