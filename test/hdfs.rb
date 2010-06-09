#!/usr/bin/env ruby
require 'AWS'

class Cluster
  @@clusters = {}
  def initialize( name, options = {} )
    
    raise ArgumentError, 
    "Cluster name '#{name}' is already in use for cluster:\n#{@@clusters[name]}\n" if @@clusters[name]

    options = { 
      :num_region_servers => 5,
      :num_zookeepers => 1
    }.merge(options)
    
    @name = name
    @num_region_servers = options[:num_region_servers]
    @num_zookeepers = options[:num_zookeepers]

    @state = "Initialized"

    puts "Cluster '#{@name}' ready to launch()."
    
    @@clusters[name] = self

  end

  def Cluster.all
    @@clusters
  end

  def Cluster.[](name) 
    @@clusters[name]
  end

  def launch()
    if fork
      #parent.
      puts "forked process to launch cluster: #{@name}.."
      @state = "Launching"
      trap("CLD") do
        pid = Process.wait
        puts "Child pid #{pid}: finished launching"
        @state = "Running"
      end

    else
      #child
      exec("~/hbase-ec2/bin/hbase-ec2 launch-cluster #{@name} #{@num_region_servers} #{@num_zookeepers}")
    end
  end

  def run_test(name)
  end

  def terminate
    if fork
      #parent.
      puts "forked process to terminate cluster: #{@name}.."
      @state = "Terminating"
      trap("CLD") do
        pid = Process.wait
        puts "Child pid #{pid}: finished terminating"
        @state = "Terminated"
      end

    else
      #child
      exec("~/hbase-ec2/bin/hbase-ec2 terminate-cluster #{@name}")
    end
  end

  def to_s
    "Cluster (state='#{@state}'): name: #@name; #region servers: #@num_region_servers; #zoo keepers: #@num_zookeepers"
  end

end



