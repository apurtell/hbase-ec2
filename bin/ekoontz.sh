#!/usr/bin/env bash

#set -x

# allow override of SLAVE_INSTANCE_TYPE from the command line 
[ ! -z $1 ] && SLAVE_INSTANCE_TYPE=$1

# Import variables
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/hbase-ec2-env.sh

echo "trying to connect using $SSH_OPTS."
