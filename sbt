#!/bin/sh
test -f ~/.sbtconfig && . ~/.sbtconfig
exec java -Xmx1024M ${SBT_OPTS} -jar `dirname $0`/lib/sbt-0.12.0-launch.jar "$@"
