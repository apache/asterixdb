#!/bin/bash

ps -ef | awk '/java.*Driver/ {print $2}' | xargs kill -9
