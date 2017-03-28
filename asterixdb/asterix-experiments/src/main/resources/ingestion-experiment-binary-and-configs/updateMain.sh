#
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
#
rm -rf /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/ingestion-experiment-binary-and-configs.zip -d /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/asterix-experiments-0.8.7-SNAPSHOT-binary-assembly.zip -d /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/;  
rm -rf /scratch/youngsk2/spatial-index-experiment/asterix-instance/* /scratch/youngsk2/spatial-index-experiment/asterix-instance/.installer
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/asterix-installer-0.8.7-SNAPSHOT-binary-assembly.zip -d /scratch/youngsk2/spatial-index-experiment/asterix-instance/;
cp -rf /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/managix-conf.xml /scratch/youngsk2/spatial-index-experiment/asterix-instance/conf/
