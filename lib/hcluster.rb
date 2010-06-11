#!/usr/bin/env ruby
require 'AWS'
require 'net/ssh'

class HClusterStateError < StandardError
end

class AWS::EC2::Base::HCluster < AWS::EC2::Base
  @@clusters = {}

  def initialize( name, options = {} )
    super(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])
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

  def status
    retval = {}
    retval['state'] = @state
    retval['num_zookeepers'] = @num_zookeepers
    retval['num_regionservers'] = @num_regionservers
    retval['launchTime'] = @launchTime
    retval['dnsName'] = @dnsName
    if @master
      retval['master'] = @master.instanceId
    end
    retval
  end

  def state 
    return @state
  end

  def sync
    #instance method: update 'self' with all info related to EC2 instances
    # where security_group = @name

    i = 0
    describe_instances.reservationSet['item'].each do |ec2_instance_set|
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

    self.status

  end

  def HCluster.status
    if @@clusters.size > 0
      instances = @@clusters[@@clusters.first[0]].describe_instances
      status_do(instances)
    else 
      temp = HCluster.new("temp")
      retval = status_do(temp.describe_instances)
      @@clusters.delete("temp")
      retval
    end
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

      channel = ssh.open_channel do |ch|

        channel.exec(command) do |ch, success|
          #FIXME: throw exception(?)
          puts "error: could not execute command '#{command}'" unless success
        end

        channel.on_data do |ch, data|
          puts "#{data}"
          channel.send_data "something for stdin\n"
        end
        
        channel.on_extended_data do |ch, type, data|
          puts "(stderr): #{data}"
        end
        
        channel.on_close do |ch|
          # cleanup, if any..
        end
      end
      
      channel.wait

    end
  end

  def run_test(test)
    #fixme : fix hardwired version (first) then path to hadoop (later)
    ssh("/usr/local/hadoop-0.20-tm-2/bin/hadoop jar /usr/local/hadoop/hadoop-test-0.20-tm-2.jar #{test}")
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

  private
  def HCluster.status_do(instances)
    retval = []
    instances.reservationSet['item'].each do |ec2_instance_set|
      security_group = ec2_instance_set.groupSet['item'][0]['groupId']
      if (security_group =~ /-zk$/)
      else
        if (security_group =~ /-master$/) 
        else
          registered_cluster = @@clusters[security_group]
          if !registered_cluster
            registered_cluster = HCluster.new(security_group)
          end
          registered_cluster.sync
          retval.push(registered_cluster.status)
        end
      end
    end
    return retval
  end

end



