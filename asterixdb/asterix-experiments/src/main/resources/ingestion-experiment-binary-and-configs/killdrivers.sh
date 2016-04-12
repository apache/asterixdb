#!/bin/bash

jps | grep Driver | awk '{print $1}' | xargs kill -9
