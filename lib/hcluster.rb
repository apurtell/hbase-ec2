#!/usr/bin/env ruby
require 'AWS'
require 'net/ssh'

class HClusterStateError < StandardError
end

class AWS::EC2::Base::HCluster < AWS::EC2::Base
  @@clusters = {}
  @@clusters_info = {}

  @@connection = AWS::EC2::Base.new(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])

  def initialize( name, options = {} )
    
    raise ArgumentError, 
    "HCluster name '#{name}' is already in use for cluster:\n#{@@clusters[name]}\n" if @@clusters[name]

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

    puts "HCluster '#{@name}' state: #{@state}"


  end

  def HCluster.status

    if @@clusters.size == 0
      #try to get cluster info from AWS if 
      #there's nothing here.
      HCluster.sync
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
    retval['master'] = @master.instanceId
    retval
  end


  def HCluster.sync
    #fixme: make synchronized, since we modify shared Class variable @@clusters_info.
    #re-initialize class variables (@@clusters) from Amazon source info.
    #get all clusters
    #for each cluster, set state.
    @@clusters_info = HCluster.describe_instances

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
            @@clusters[security_group] = HCluster.new(security_group)
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

    HCluster.status
  end

  def state 
    return @state
  end

  def sync
    #fixme: make write-synchronized, since we read shared Class variable @@clusters_info.
    #(multiple read-only accessors, (like this function) are fine, though).
    #instance method: update 'self' with all info related to EC2 instances
    # where security_group = @name
    @@clusters_info = HCluster.describe_instances

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

  def HCluster.describe_instances(options = {})
    # class method: get all instances from @@connection.
    @@connection.describe_instances(options)
  end

  def HCluster.describe_security_groups(options = {})
    # class method: get all instances from @@connection.
    @@connection.describe_security_groups(options)
  end

  def describe_instances
    # object method: get all instances from @@connection with security_group = @name.
  end

  def HCluster.[](name) 
    test = @@clusters[name]
    if test
      test
    else
      @@clusters[name] = HCluster.new(name)
    end
  end

  def launch()
    #kill existing 'launch' threads for this cluster, if any.
    # ..
    
    thread = Thread.new(@name) do |cluster_name|
      puts "new thread to launch cluster: #{cluster_name}.."
      @state = "launching"
      # (use pure-ruby AWS call here rather than the following):
      # exec("~/hbase-ec2/bin/hbase-ec2 launch-cluster #{@name} #{@num_region_servers} #{@num_zookeepers}")
    end

  end

  def ssh(command)
    raise HClusterStateError,
    "HCluster '#{name}' is not in running state:\n#{self.to_s}\n" if @state != 'running'
    
    # http://net-ssh.rubyforge.org/ssh/v2/api/classes/Net/SSH.html#M000013
    # paranoid=>false because we should ignore known_hosts, since AWS IPs get frequently recycled
    # and their servers' private keys will vary.
    Net::SSH.start(@dnsName,'root',
                   :keys => ["~/.ec2/root.pem"],
                   :paranoid => false
                   ) do |ssh|
      stdout = ""
      ssh.exec!(command) do |channel,stream,data|
        stdout << data if stream == :stdout
      end
      puts stdout
    end
  end

  def run_test(test)
    puts "starting test.."
    ssh("/usr/local/hadoop-0.20-tm-2/bin/hadoop jar /usr/local/hadoop/hadoop-test-0.20-tm-2.jar #{test}")
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
    "HCluster (state='#{@state}'): name: #@name; #region servers: #@num_region_servers; #zoo keepers: #@num_zookeepers"
  end

end



