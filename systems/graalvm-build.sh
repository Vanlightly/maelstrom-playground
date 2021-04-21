#!/usr/bin/env bash
set -ex


$GRAALVM_HOME/bin/native-image -jar systems-0.1-SNAPSHOT.jar bkkvs --no-fallback