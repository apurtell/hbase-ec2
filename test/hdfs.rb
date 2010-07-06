#!/usr/bin/ruby

require 'test/unit'
$:.unshift File.join(File.dirname(__FILE__),"..", "lib")
require 'TestDFSIO.rb'

def dump_hash(hash)
  hash.keys.each { |key|
    puts "#{key} => #{hash[key]}"
  }
end

class TestHCluster < Test::Unit::TestCase
  @@security_group = "hdfs"
  @@num_zookeepers = 1
  @@num_regionservers = 3
  @@cluster = HCluster::TestDFSIO.new({:security_group_prefix => @@security_group,
                                        :num_zookeepers => @@num_zookeepers,
                                        :num_regionservers => @@num_regionservers})

  def setup
    @@cluster.launch
  end

  def teardown
    @@cluster.terminate
  end

  def test_run
    status = @@cluster.status
    dump_hash(status)
    assert("running" == status['state'])
    launchTime = status['launchTime']
    # second part of this disjunction is indended to only be true if launchTime is
    # a valid EC-2 returned launching time, e.g.: "2010-06-17T21:03:56.000Z"
    assert((nil == launchTime) || Time.parse(Time.parse(launchTime).to_s) == Time.parse(launchTime))

    # make sure number of zookeepers is the same as number asked for.
    assert(@@num_zookeepers == @@cluster.zks.size)

    # make sure number of zookeepers is the same as number asked for.
    assert(@@num_regionservers == @@cluster.slaves.size)

    #FIX: add some tests for master..

    test_results = @@cluster.test
    assert(0 < test_results.size)
    assert(10000 == test_results['Total MBytes processed'])
  end
  
end
