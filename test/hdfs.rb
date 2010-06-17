#!/usr/bin/ruby

require 'test/unit'

load("~/hbase-ec2/lib/hcluster.rb");

class TestHCluster < Test::Unit::TestCase
  def test_init
    hdfs = AWS::EC2::Base::HCluster.new("hdfs");
    # show status
    status = hdfs.status
    assert_equal("hdfs",status['name'])
    assert(("terminated" == status['state']) || ("Initialized" == status['state']))
  end
end


#hdfs.launch
#hdfs.run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")
#hdfs.terminate



