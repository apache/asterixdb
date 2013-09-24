rm -rf dist
mkdir dist

hadoop_versions=(0.20.2 0.23.1 0.23.6 1.0.4 cdh-4.1 cdh-4.2)
cd ../
for v in ${hadoop_versions[@]}
do
   #echo mvn clean package -DskipTests=true -Dhadoop=${v}
   mvn clean package -DskipTests=true -Dhadoop=${v}
   #echo mv pregelix/pregelix-dist/target/pregelix-dist-*-binary-assembly.zip pregelix/dist/pregelix-dist-binary-assembley-hdfs-${v}.zip
   mv pregelix/pregelix-dist/target/pregelix-dist-*-binary-assembly.zip pregelix/dist/pregelix-dist-binary-assembley-hdfs-${v}.zip
done
