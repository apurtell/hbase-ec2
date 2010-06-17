#!/usr/bin/ruby

require 'test/unit'

load("~/hbase-ec2/lib/hcluster.rb");

class TestHCluster < Test::Unit::TestCase
  def test_init

    security_group = "hdfs"

    hdfs = AWS::EC2::Base::HCluster.new(security_group)
    # show status
    status = hdfs.status

    status.keys.each { |key|
      puts "#{key} => #{status[key]}"
    }

    assert_equal(security_group,status['name'])
    assert(("terminated" == status['state']) || ("Initialized" == status['state']))
    assert((nil == status['launchTime']) || ("2010-06-17T18:45:13.000Z" == status['launchTime']))
  end
end


#hdfs.launch
#hdfs.run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")
#hdfs.terminate



