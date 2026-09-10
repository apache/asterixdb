#!/bin/bash
cd /var/tmp/asterix-app/
shiv -o target/TweetSent.pyz --site-packages src/test/resources/TweetSent --prefer-binary numpy
