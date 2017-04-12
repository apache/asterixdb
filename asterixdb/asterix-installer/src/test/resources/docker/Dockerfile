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
RUN yum install -y unzip java-1.8.0-openjdk openssh-server openssh-clients python-setuptools wget curl net-tools

RUN sshd-keygen
RUN sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config

RUN useradd asterix -G wheel -s /bin/bash -m
RUN echo 'asterix:asterix' | chpasswd
RUN echo '%wheel ALL=(ALL) ALL' >> /etc/sudoers
RUN echo 'Host *' >> /etc/ssh/ssh_config
RUN echo '    StrictHostKeyChecking no' >> /etc/ssh/ssh_config

CMD ["/usr/sbin/sshd", "-D"]

RUN mkdir /asterixdb
RUN mkdir /asterixdb/asterix
RUN mkdir /asterixdb/data
COPY asterix-installer*.zip .
RUN unzip asterix-installer*.zip -d /asterixdb/asterix-installer/
COPY id_rsa /root/.ssh/id_rsa
COPY id_rsa.pub /root/.ssh/id_rsa.pub
COPY cluster-config.xml /asterixdb/

RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/id_rsa*
RUN chmod 700 /root/.ssh/authorized_keys

ENV JAVA_HOME /usr/lib/jvm/jre-1.8.0
ENV JAVA_OPTS -Xmx2048m

EXPOSE 19001 19002 8888 19003 50031 15001
