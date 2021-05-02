#!/usr/bin/env bash
set -ex

cd build/libs
$GRAALVM_HOME/bin/native-image -jar bkkvs-0.1-SNAPSHOT.jar bkkvs