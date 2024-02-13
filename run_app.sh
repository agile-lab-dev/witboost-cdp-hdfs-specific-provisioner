#!/bin/bash

exec java -javaagent:opentelemetry-javaagent.jar -Djava.security.krb5.conf=/opt/docker/etc/configs/krb5.conf -jar cdp-private-hdfs-specific-provisioner.jar