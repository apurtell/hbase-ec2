options = {
  :label => 'hbase-0.20-tm-3-x86_64', 
  :availability_zone => 'us-east-1c',
  :num_regionservers => 3,
  :owner_id => '801535628028'
}
cluster = @hcluster.new options
cluster.launch

cluster.ssh("cd /usr/local/hadoop-*; kinit -k -t conf/nn.keytab hadoop/#{cluster.master.privateDnsName.downcase}; bin/hadoop fs -mkdir /hbase; bin/hadoop fs -chown hbase /hbase")
cluster.ssh("/usr/local/hbase-*/bin/hbase-daemon.sh start master")
cluster.slaves.each {|slave|
  @hcluster.ssh_to(slave.dnsName, "/usr/local/hbase-*/bin/hbase-daemon.sh start regionserver")}
