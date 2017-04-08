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

## <a id="Introduction">Introduction</a>
   Note that you can always manually launch a number of Amazon Web Services EC2 instances and then run the
   Ansible cluster installation scripts as described [here](ansible.html) separately to manage the
   lifecycle of an AsterixDB cluster on those EC2 instances.

   However, via this installation option, we provide a combo solution for automating both AWS EC2
   and AsterixDB, where you can run only one script to deploy, start, stop, and terminate
   an AsterixDB cluster on AWS.

## <a id="Prerequisites">Prerequisites</a>
  *  Supported operating systems for the client: **Linux** and **MacOS**

  *  Supported operating systems for Amazon Web Services instances: **Linux**

  *  Install pip on your client machine:

     CentOS

         $ sudo yum install python-pip

     Ubuntu

         $ sudo apt-get install python-pip

     macOS

         $ brew install pip

  *  Install Ansible, boto, and boto3 on your client machine:

         $ pip install ansible
         $ pip install boto
         $ pip install boto3

     Note that you might need `sudo` depending on your system configuration.

     **Make sure that the version of Ansible is no less than 2.2.1.0**:

         $ ansible --version
         ansible 2.2.1.0

     **For users with macOS 10.11+**, please create a user-level Ansible configuration file at:

         ~/.ansible.cfg

     and add the following configuration:

         [ssh_connection]
         control_path = %(directory)s/%%C

  *  Download the AsterixDB distribution package, unzip it, navigate to `opt/aws/`

         $ cd opt/aws

     The following files and directories are in the directory `opt/aws`:

         README  bin  conf  yaml

     `bin` contains scripts that start and terminate an AWS-based cluster instance, according to the configuration
     specified in files under `conf`, and `yaml` contains internal Ansible scripts that the shell scripts in `bin` use.

  *  Create an AWS account and an IAM user.

     Set up a security group that you'd like to use for your AWS cluster.
     **The security group should at least allow all TCP connections from anywhere.**
     Provide the name of the security group as the value for the `group` field in `conf/aws_settings.yml`.

  *  Retrieve your AWS EC2 key pair name and use that as the `keypair` in `conf/aws_settings.yml`;

     retrieve your AWS IAM `access key ID` and use that as the `access_key_id` in `conf/aws_settings.yml`;

     retrieve your AWS IAM `secret access key` and use that as the `secret_access_key` in `conf/aws_settings.yml`.

     Note that you can only read or download `access key ID` and `secret access key` once from your AWS console.
     If you forget them, you have to create new keys and delete the old ones.

  *  Configure your ssh setting by editing `~/.ssh/config` and adding the following entry:

         Host *.amazonaws.com
              IdentityFile <path_of_private_key>

     Note that \<path_of_private_key\> should be replaced by the path to the file that stores the private key for the
     key pair that you uploaded to AWS and used in `conf/aws_settings`. For example:

         Host *.amazonaws.com
              IdentityFile ~/.ssh/id_rsa

## <a id="config">Cluster Configuration</a>
  *  **AWS settings**.  Edit `conf/instance_settings.yml`. The meaning of each parameter is listed as follows:

         # The OS image id for ec2 instances.
         image: ami-76fa4116

         # The data center region for ec2 instances.
         region: us-west-2

         # The tag for each ec2 machine. Use different tags for isolation.
         tag: scale_test

         # The name of a security group that appears in your AWS console.
         group: default

         # The name of a key pair that appears in your AWS console.
         keypair: <to be filled>

         # The AWS access key id for your IAM user.
         access_key_id: <to be filled>

         # The AWS secret key for your IAM user.
         secret_access_key: <to be filled>

         # The AWS instance type. A full list of available types are listed at:
         # https://aws.amazon.com/ec2/instance-types/
         instance_type: t2.micro

         # The number of ec2 instances that construct a cluster.
         count: 3

         # The user name.
         user: ec2-user

         # Whether to reuse one slave machine to host the master process.
         cc_on_nc: false

      **As described in [prerequisites](#Prerequisites), the following parameters must be customized:**

         # The tag for each ec2 machine. Use different tags for isolation.
         tag: scale_test

         # The name of a security group that appears in your AWS console.
         group: default

         # The name of a key pair that appears in your AWS console.
         keypair: <to be filled>

         # The AWS access key id for your IAM user.
         access_key_id: <to be filled>

         # The AWS secrety key for your IAM user.
         secret_access_key: <to be filled>

  *  **Remote working directories**. Edit `conf/instance_settings.yml` to change the remote binary directory
     (the variable "binarydir") when necessary. By default, the binary directory will be under the home directory
     (as the value of Ansible builtin variable ansible_env.HOME) of the ssh user account on each node.


## <a id="lifecycle">Cluster Lifecycle Management</a>
  *  Allocate AWS EC2 nodes (the number of nodes is specified in `conf/instance_settings.yml`)
     and deploy the binary to all allocated EC2 nodes:

         bin/deploy.sh

  *  Before starting the AsterixDB cluster, you the instance configuration file `conf/instance/cc.conf`
     can be modified with the exception of the IP addresses/DNS names which are are generated and cannot
     be changed. All available parameters and their usage can be found [here](ncservice.html#Parameters).

  *  Launch your AsterixDB cluster on EC2:

         bin/start.sh

     Now you can use the multi-node AsterixDB cluster on EC2 by by opening the master node
     listed in `conf/instance/inventory` at port `19001` (which can be customized in `conf/instance/cc.conf`)
     in your browser.

  *  If you want to stop the AWS-based AsterixDB cluster, run the following script:

         bin/stop.sh

     Note that this only stops AsterixDB but does not stop the EC2 nodes.

  *  If you want to terminate the EC2 nodes that run the AsterixDB cluster, run the following script:

         bin/terminate.sh

     **Note that it will destroy everything in the AsterixDB cluster you installed and terminate all EC2 nodes
     for the cluster.**
