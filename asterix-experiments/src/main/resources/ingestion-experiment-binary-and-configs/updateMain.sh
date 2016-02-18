rm -rf /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/  
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/ingestion-experiment-binary-and-configs.zip -d /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/asterix-experiments-0.8.7-SNAPSHOT-binary-assembly.zip -d /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/;  
rm -rf /scratch/youngsk2/spatial-index-experiment/asterix-instance/* /scratch/youngsk2/spatial-index-experiment/asterix-instance/.installer
unzip /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/asterix-installer-0.8.7-SNAPSHOT-binary-assembly.zip -d /scratch/youngsk2/spatial-index-experiment/asterix-instance/;
cp -rf /home/youngsk2/spatial-index-experiment/ingestion-experiment-binary-and-configs/managix-conf.xml /scratch/youngsk2/spatial-index-experiment/asterix-instance/conf/
