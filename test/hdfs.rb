#!/usr/bin/ruby

require 'test/unit'
$:.unshift File.join(File.dirname(__FILE__),"..", "lib")
require 'hcluster'

def dump_hash(hash)
  hash.keys.each { |key|
    puts "#{key} => #{hash[key]}"
  }
end

class TestHCluster < Test::Unit::TestCase
  @@security_group = "hdfs"
  @@cluster = nil

  def test_init

    @@cluster = AWS::EC2::Base::HCluster.new(@@security_group)
    # show status
    status = @@cluster.status

    dump_hash(status)

    assert_equal(@@security_group,status['name'])

    if ("running" == status['state'])
      puts "terminating running cluster.."
      @@cluster.terminate
      sleep 10
      puts "continuing."

      status = @@cluster.status
#      dump_hash(status)      
    end

    assert(("terminated" == status['state']) || ("Initialized" == status['state']) || ("shutting-down" == status['state']))
    launchTime = status['launchTime']
    # second part of this disjunction is indended to only be true if launchTime is
    # a valid EC-2 returned launching time, e.g.: "2010-06-17T21:03:56.000Z"
    assert((nil == launchTime) || Time.parse(Time.parse(launchTime).to_s) == Time.parse(launchTime))

  end

  def test_launch
    puts "test: launch.."
    @@cluster.launch
    assert(true)
  end

  def test_zookeepers
    # make sure number of zookeepers is the same as number asked for.
    assert(@@num_zookeepers = @@cluster.zks.instancesSet['item'].size)
  end

  def test_master
    # for now, only test is check for existence of id_rsa file on master.
    found_key = false

    @@cluster.ssh_to(@@cluster.master.dnsName,
                     "ls -l .ssh | grep id_rsa",
                     lambda{|line| 
                       if line =~ /id_rsa/
                         found_key = true
                         puts "found key.."
                       end
                     })
    assert(found_key)
  end

  def test_regionservers
    assert(true)
  end

  def test_stuff
    puts "test: work.."
    #work goes here..
    sleep 5
    puts "done."
  end

  def test_terminate
    puts "test: terminate.."
    @@cluster.terminate
    assert(true)
  end

  

end


#hdfs.launch
#hdfs.run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")
#hdfs.terminate



