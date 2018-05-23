#!/bin/bash

ON_JAVA_8=$(echo $JAVA_HOME | grep java-8)

if [[ -z $ON_JAVA_8 ]]; then
    mvn clean compile
else
    echo "on JDK 8"
    mvn clean verify -Pcoverage
fi
