<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

* [Introduction](#Introduction)
* [Prerequisites](#Prerequisites)
* [Configuration and parameters](#config)
* [Manage the lifecycle of your instance](#lifecycle)

## <a id="Introduction">Introduction</a>
This installation option wraps the basic, low-level installation binaries described in the [NCService
installation option](ncservice.html), and provides several wrapped [Ansible](https://www.ansible.com/)-based
scripts to deploy, start, stop, and erase an AsterixDB instance on a multi-node cluster without requiring
users to interact with each individual node in the cluster.

## <a id="Prerequisites">Prerequisites</a>
  *  Supported operating systems: **Linux** and **MacOS**

  *  Install pip on your client machine:

         CentOS: sudo yum install python-pip
         Ubuntu: sudo apt-get install python-pip
         MacOS:  brew install pip

  *  Install Ansible, boto, and boto3 on your client machine:

         pip install ansible
         pip install boto
         pip install boto3

     **Make sure that the version of Ansible is no less than 2.2.1.0.**

  *  Download a released [simple server package](http://asterixdb.apache.org/download.html).

     Alternatively, you can follow the [instruction](https://github.com/apache/asterixdb#build-from-source) to
     build from source.

  *  In the extracted directory from the `simple server package`, navigate to `opt/ansible/`

         $cd opt/ansible

     The following files and directories are in the directory `opt/ansible`:

         README  bin  conf  yaml

     `bin` contains scripts that deploy, start, stop and erase a multi-node AsterixDB instance, according to
     the configuration specified in files under `conf/`. `yaml` contains internal Ansible scripts that the shell
     scripts in `bin` use.


## <a id="config">Configuration and parameters</a>
  *  **Parameters**. Edit the instance configuration file `conf/cc.conf` when necessary.
     You can add/update whatever parameters in the **[common]** and **[nc]** sections (except IPs and ports).
     For example:

         [common]
         log.level=INFO

         [nc]
         txn.log.dir=txnlog
         iodevices=iodevice
         command=asterixnc

     More parameters and their usage can be found [here](ncservice.html#Parameters).
     Note that with this installation option, all parameters in the **[cc]** and **[ncservice]**
     section will use defaults and cannot be changed.


  *  **Nodes and account**. Edit the inventory file `conf/inventory` when necessary.
     You mostly only need to sepecify the node DNS names (or IPs) for the cluster controller, i.e., the master node,
     in the **[cc]** section, and node controllers, i.e., slave nodes, in the **[ncs]** section.
     The following example configures a cluster with two slave nodes (172.0.1.11 and 172.0.1.12) and
     one master node (172.0.1.10).

         [cc]
         172.0.1.10

         [ncs]
         172.0.1.11
         172.0.1.12

     **Configure passwordless ssh from your current client that runs the scripts to all nodes listed
     in `conf/inventory`.**
     If the ssh user account for target machines is different from your current username, please uncomment
     and edit the following two lines:

         ;[all:vars]
         ;ansible_ssh_user=<fill with your ssh account username>

     If you want to specify advanced Ansible builtin variables, please refer to the
     [Ansible documentation](http://docs.ansible.com/ansible/intro_inventory.html).

  *  **Remote working directories**. Edit `conf/instance_settings.yml` to change the instance binary directories
     when necessary. By default, the binary directory will be under the home directory (as the value of
     Ansible builtin variable ansible_env.HOME) of the ssh user account on each node.

         # The parent directory for the working directory.
         basedir: "{{ ansible_env.HOME }}"

         # The working directory.
         binarydir: "{{ basedir }}/{{ product }}"


## <a id="lifecycle">Manage the lifecycle of your instance</a>
  *  Deploy the AsterixDB binary to all nodes:

         bin/deploy.sh

  *  Launch your cluster instance:

         bin/start.sh

     Now you can use the multi-node AsterixDB instance.

  * If you want to stop the the multi-node AsterixDB instance, run the following script:

         bin/stop.sh

  * If you want to remove the AsterixDB binary on all nodes, run the following script:

         bin/erase.sh
