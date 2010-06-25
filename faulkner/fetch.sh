bin=~/hbase-ec2-2/bin

for i in `$bin/list-hbase-master $1` `$bin/list-hbase-slaves $1` `$bin/list-hbase-zookeeper $1` `$bin/list-hbase-aux $1` ; do ( mkdir -p $i ; cd $i ; rsync -avz --exclude history -e 'ssh -i /home/hadoop/.ec2/root-rsa.pem -o StrictHostKeyChecking=no' root@$i:/mnt/h*/logs/* . ); done
