#!/usr/bin/env bash
set -ex

$GRAALVM_HOME/bin/javac Main.java
$GRAALVM_HOME/bin/native-image Main

#javac ExtListDir.java
#$GRAALVM_HOME/bin/native-image --js ExtListDir