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
  @@cluster = AWS::EC2::Base::HCluster.new(@@security_group)

  def setup
    @@cluster.launch
  end

  def teardown
    @@cluster.terminate
  end

  def test_run
    status = @@cluster.status
    dump_hash(status)
    assert_equal(@@security_group,status['name'])
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

    test_results = @@cluster.hdfs_test
    assert(0 < test_results.size)
    assert(10000 == test_results['Total MBytes processed'])
  end
  
end
