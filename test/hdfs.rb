#!/usr/bin/env ruby
require 'AWS'

class Cluster

  def initialize( options = {} )
    options = { 
      :num_region_servers => 5,
      :num_zookeepers => 1
    }.merge(options)
    
    @name = options[:name]
    @num_region_servers = options[:num_region_servers]
    @num_zookeepers = options[:num_zookeepers]

    raise ArgumentError, "No :name provided" if options[:name].nil? || options[:name].nil?

    puts "Cluster '#{@name}' ready to launch()."

  end

  def launch()
    if fork
      #parent.
      puts "forked process to launch cluster: #{@name}.."
    else
      #child
      exec("~/hbase-ec2/bin/hbase-ec2 launch-cluster #{@name} #{@num_region_servers} #{@num_zookeepers}")
    end
  end

  def run_test(name)
  end

  def terminate
    exec("~/hbase-ec2/bin/hbase-ec2 terminate-cluster #{@name}")
    puts "stopping cluster: #{@name}"
  end

  def to_s
    "Cluster: name=#@name; region servers: #@num_region_servers; zoo keepers: #@num_zookeepers"
  end

end



