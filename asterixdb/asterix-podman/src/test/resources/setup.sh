#!/bin/bash
cd /var/tmp/asterix-app/
shiv -o target/TweetSent.pyz --site-packages src/test/resources/TweetSent scikit-learn
cp -a /var/tmp/asterix-app/data/classifications /opt/apache-asterixdb/data/
cp -a /var/tmp/asterix-app/data/twitter /opt/apache-asterixdb/data/
cp -a /var/tmp/asterix-app/data/big-object /opt/apache-asterixdb/data/
mkdir -p /opt/apache-asterixdb/target/data/
cp -a /var/tmp/asterix-app/target/data/big-object /opt/apache-asterixdb/target/data/