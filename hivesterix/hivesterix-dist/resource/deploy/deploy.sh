#/*
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*/
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-anttasks-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-anttasks -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-cli-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-cli -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-common-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-common -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-exec-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-exec -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-hwi-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-hwi -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-jdbc-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-jdbc -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-metastore-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-metastore -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-serde-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-serde -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-service-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-service -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/hive-shims-0.7.0.jar -DgroupId=org.apache.hadoop.hive -DartifactId=hive-shims -Dversion=0.7.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/libthrift.jar -DgroupId=org.apache.thrift -DartifactId=libthrift -Dversion=0.5.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/libfb303.jar -DgroupId=com.facebook -DartifactId=libfb303 -Dversion=0.5.0 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/commons-cli-1.2.jar -DgroupId=org.apache.commons -DartifactId=cli -Dversion=1.2 -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/jdo2-api-2.3-ec.jar -DgroupId=javax -DartifactId=jdo2-api -Dversion=2.3-ec -Dpackaging=jar
mvn -e deploy:deploy-file -Durl=http://obelix.ics.uci.edu/nexus/content/repositories/third-party/ -DrepositoryId=third-party -Dfile=$HIVE_HOME/lib/log4j-1.2.15.jar -DgroupId=org.apache -DartifactId=log4j -Dversion=1.2.15 -Dpackaging=jar

