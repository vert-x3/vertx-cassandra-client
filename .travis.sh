#!/bin/bash

ON_JAVA_8=$(echo $JAVA_HOM | grep java-8)

if [[ -z $ON_JAVA_8 ]]; then
    mvn clean verify
else
    echo "on JDK 8"
    mvn clean verify -Pcoverage
fi
