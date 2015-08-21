#!/bin/bash

if [ $# -eq 1 ]; then
    if [ $1 == 'run-maven' ]; then
        mvn -T 4 clean install -DskipTests
    fi
fi

## Check if 'build' folder exists to delete the content. If not it is created.
if [ -d build ]; then
    rm -rf build/*
else
    mkdir build
fi

## Copy all the binaries, dependencies and files
cp -r bionetdb-app/target/appassembler/* build/
cp -r bionetdb-app/app/ext-libs/* build/libs
#cp  bionetdb-core/target/classes/configuration.json build/
cp bionetdb-server/target/bionetdb.war build/
cp README.md build/
cp LICENSE build/

