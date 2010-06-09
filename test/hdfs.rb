#!/usr/bin/env ruby
require 'AWS'

class Cluster

  def initialize( options = {} )
    options = { 
      :num_region_servers => 5,
      :num_zookeepers => 0
    }.merge(options)
    
    @name = options[:name]
    @num_region_servers = options[:num_region_servers]
    @num_zookeepers = options[:num_zookeepers]

    raise ArgumentError, "No :name provided" if options[:name].nil? || options[:name].nil?
  end

  def launch()


  end

  def run_test(name)
  end

  def terminate()
  end
end

puts 'hello'
mycluster = Cluster.new(:name => "hdfs")
puts mycluster.to_s

