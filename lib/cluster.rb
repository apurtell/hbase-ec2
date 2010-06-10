#!/usr/bin/env ruby
require 'AWS'
require 'net/ssh'

class ClusterStateError < StandardError
end

class Cluster
  @@clusters = {}
  @@clusters_info = {}

  @@connection = AWS::EC2::Base.new(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])

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
    @@clusters[name] = self

    @zks = []
    @master = nil
    @slaves = []

    @state = "Initialized"
    sync

    puts "Cluster '#{@name}' state: #{@state}"


  end

  def Cluster.status

    if @@clusters.size == 0
      #try to get cluster info from AWS if 
      #there's nothing here.
      Cluster.sync
    end

    retval = {}
    @@clusters.each  do |name,cluster|
      retval[name] = cluster.status
    end
    retval
  end

  def status
    retval = {}
    retval['state'] = @state
    retval['num_zookeepers'] = @num_zookeepers
    retval['num_regionservers'] = @num_regionservers
    retval['launchTime'] = @launchTime
    retval['dnsName'] = @dnsName
    retval
  end


  def Cluster.sync
    #fixme: make synchronized, since we modify shared Class variable @@clusters_info.
    #re-initialize class variables (@@clusters) from Amazon source info.
    #get all clusters
    #for each cluster, set state.
    @@clusters_info = Cluster.describe_instances

    i = 0
    @@clusters_info.reservationSet['item'].each do |ec2_instance_set|
      security_group = ec2_instance_set.groupSet['item'][0]['groupId']
      if security_group !~ /-zk$/ and security_group !~ /-master$/
        # check master only (not regionservers or zookeepers)
        if (ec2_instance_set['instancesSet']['item'][0]['instanceState']['name'] == "terminated") 
          puts "ignoring terminated instance with security group: '#{security_group}'"
        else
          if @@clusters[security_group] == nil
            puts "creating in-memory cluster record : '#{security_group}'"
            @@clusters[security_group] = Cluster.new(security_group)
          end
          puts "syncing: : '#{security_group}'"
          @@clusters[security_group].sync
        end
      end
      i = i+1
    end

    #remove any member of @@cluster whose state == "terminated"
    @@clusters.each do |name,cluster|
      if (cluster.state == "terminated")
        @@clusters.delete(name)
      end
    end

    Cluster.status
  end

  def state 
    return @state
  end

  def sync
    #fixme: make write-synchronized, since we read shared Class variable @@clusters_info.
    #(multiple read-only accessors, (like this function) are fine, though).
    #instance method: update 'self' with all info related to EC2 instances
    # where security_group = @name
    @@clusters_info = Cluster.describe_instances

    i = 0
    @@clusters_info.reservationSet['item'].each do |ec2_instance_set|
      security_group = ec2_instance_set.groupSet['item'][0]['groupId']
      if (security_group == @name)
        @slaves = ec2_instance_set['instancesSet']['item']
      else
        if (security_group == (@name + "-zk"))
          @zks = ec2_instance_set['instancesSet']['item']
        else
          if (security_group == (@name + "-master"))
            @master = ec2_instance_set['instancesSet']['item'][0]
            @state = @master['instanceState']['name']
            @dnsName = @master['dnsName']
            @launchTime = @master['launchTime']
          end
        end
      end
      i = i+1
    end

    @num_zookeepers = @zks.size
    @num_regionservers = @slaves.size

  end

  def Cluster.describe_instances(options = {})
    # class method: get all instances from @@connection.
    @@connection.describe_instances(options)
  end

  def Cluster.describe_security_groups(options = {})
    # class method: get all instances from @@connection.
    @@connection.describe_security_groups(options)
  end

  def describe_instances
    # object method: get all instances from @@connection with security_group = @name.
  end

  def Cluster.[](name) 
    @@clusters[name]
  end

  def launch()
    #kill existing 'launch' threads for this cluster, if any.
    # ..

    if fork
      #parent.
      puts "forked process to launch cluster: #{@name}.."
      @state = "launching"
      trap("CLD") do
        pid = Process.wait
        puts "Child pid #{pid}: finished launching"
        sync
      end

    else
      #child
      exec("~/hbase-ec2/bin/hbase-ec2 launch-cluster #{@name} #{@num_region_servers} #{@num_zookeepers}")
    end
  end

  def run_test(name)
    raise ClusterStateError,
    "Cluster '#{name}' is not in running state:\n#{self.to_s}\n" if @state != 'running'

    puts "starting test.."
    
    # http://net-ssh.rubyforge.org/ssh/v2/api/classes/Net/SSH.html#M000013
    # paranoid=>false because we should ignore known_hosts, since AWS IPs get frequently recycled
    # and their servers' private keys will vary.
    Net::SSH.start(@dnsName,'root',
                   :keys => ["~/.ec2/root.pem"],
                   :paranoid => false
                   ) do |ssh|
      puts "inner loop..."
      stdout = ""
      ssh.exec!("ls -l /") do |channel,stream,data|
        stdout << data if stream == :stdout
      end
      puts stdout

    end
    puts "done."

  end

  def terminate
    #kill 'launch' threads for this cluster, if any.
    # ..
    

    if fork
      #parent.
      puts "forked process to terminate cluster: #{@name}.."
      @state = "terminating"
      trap("CLD") do
        pid = Process.wait
        puts "Child pid #{pid}: finished terminating"
        @state = "terminated"
      end

    else
      #child
      exec("~/hbase-ec2/bin/hbase-ec2 terminate-cluster #{@name} noprompt")
    end
  end

  def to_s
    "Cluster (state='#{@state}'): name: #@name; #region servers: #@num_region_servers; #zoo keepers: #@num_zookeepers"
  end

end



