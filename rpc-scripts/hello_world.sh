#!/bin/sh

DATE=`/bin/date`
/bin/echo "Hello world: $DATE" # to stdout
/bin/echo "Hello world: $DATE" > /tmp/hello_world.txt # to file
