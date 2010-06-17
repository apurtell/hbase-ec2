#!/usr/bin/ruby

require 'test/unit'

load("~/hbase-ec2/lib/hcluster.rb");

class TestHCluster < Test::Unit::TestCase
  @@security_group = "hdfs"
  @@cluster = nil

  def test_init

    @@cluster = AWS::EC2::Base::HCluster.new(@@security_group)
    # show status
    status = @@cluster.status

    status.keys.each { |key|
      puts "#{key} => #{status[key]}"
    }

    assert_equal(@@security_group,status['name'])

    if ("running" == status['state'])
      puts "terminating running cluster.."
      @@cluster.terminate
      sleep 10
      puts "continuing."

      status = @@cluster.status
      
      status.keys.each { |key|
        puts "#{key} => #{status[key]}"
      }
     
    end

    assert(("terminated" == status['state']) || ("Initialized" == status['state']) || ("shutting-down" == status['state']))
    launchTime = status['launchTime']
    # second part of this disjunction is indended to only be true if launchTime is
    # a valid EC-2 returned launching time, e.g.: "2010-06-17T21:03:56.000Z"
    assert((nil == launchTime) || Time.parse(Time.parse(launchTime).to_s) == Time.parse(launchTime))




  end



#  def test_launch
#    @@cluster.launch
#    assert(true)
#  end
  

end


#hdfs.launch
#hdfs.run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")
#hdfs.terminate



