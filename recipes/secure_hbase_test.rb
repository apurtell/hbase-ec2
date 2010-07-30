load File.dirname(__FILE__)+'/../lib/TestDFSIO.rb'
include Hadoop

options = {
  :label => 'hbase-us-east-1-0.21-S-append-SNAPSHOT-x86_64', 
  :num_regionservers => 3
}
cluster = TestDFSIO.new options
cluster.launch

cluster.ssh("cd /usr/local/hadoop-*; kinit -k -t conf/nn.keytab hadoop/#{cluster.master.privateDnsName.downcase}; bin/hadoop fs -mkdir /hbase; bin/hadoop fs -chown hbase /hbase")
cluster.ssh("/usr/local/hbase-*/bin/hbase-daemon.sh start master")
cluster.slaves.each {|slave|
  @hcluster.ssh_to(slave.dnsName, "/usr/local/hbase-*/bin/hbase-daemon.sh start regionserver")}

res = cluster.test
puts 'Results:'
puts res['pairs']
puts 'Output:'
puts res['stdout']

cluster.terminate

