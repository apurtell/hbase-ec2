#!/usr/bin/env bash
#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#FIXME: replace this shell script with a more 
# declarative statement of what we want
# the just-started zookeeper's setup to look like,
# using Whirr, Chef, Puppet, or some combination thereof.

# FIXME: allow input of LOG_SETTING (one of {'INFO','DEBUG',...}) 
# to configure zookeeper log output level, 
# as (see how hbase-ec2-init-remote.sh does it).

# ZOOKEEPER_QUORUM must be set in the environment by the caller.
HBASE_HOME=`ls -d /usr/local/hbase-* | grep -v tar.gz | head -n1`
set -x
export JAVA_HOME=/usr/local/jdk1.6.0_20

###############################################################################
# HBase configuration (Zookeeper)
###############################################################################


cat > $HBASE_HOME/conf/hbase-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>$ZOOKEEPER_QUORUM</value>
</property>
<property>
  <name>zookeeper.session.timeout</name>
  <value>60000</value>
</property>
<property>
  <name>hbase.zookeeper.property.dataDir</name>
  <value>/mnt/hbase/zk</value>
</property>
<property>
  <name>hbase.zookeeper.property.maxClientCnxns</name>
  <value>100</value>
</property>
</configuration>
EOF

# Start services

# up open file descriptor limits
echo "root soft nofile 65536" >> /etc/security/limits.conf
echo "root hard nofile 65536" >> /etc/security/limits.conf

# up epoll limits
# ok if this fails, only valid for kernels 2.6.27+
sysctl -w fs.epoll.max_user_instance=65536 > /dev/null 2>&1

mkdir -p /mnt/hbase/logs
mkdir -p /mnt/hbase/zk

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

"$HBASE_HOME"/bin/hbase-daemon.sh start zookeeper
