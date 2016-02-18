#!/bin/bash

screen -ls | grep asterix | cut -d. -f1 | awk '{print $1}' | xargs kill -9
screen -wipe
