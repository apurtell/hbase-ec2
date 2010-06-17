#!/usr/bin/env ruby
require 'AWS'
require 'net/ssh'

def trim(string = "")
  string.gsub(/^\s+/,'').gsub(/\s+$/,'')
end

class HClusterStateError < StandardError
end

class AWS::EC2::Base::HCluster < AWS::EC2::Base
  @@clusters = {}

  def initialize( name, options = {} )
    super(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])
    raise ArgumentError, 
    "HCluster name '#{name}' is already in use for cluster:\n#{@@clusters[name]}\n" if @@clusters[name]

    options = { 
      :num_regionservers => 5,
      :num_zookeepers => 1
    }.merge(options)
    
    @name = name
    @num_regionservers = options[:num_regionservers]
    @num_zookeepers = options[:num_zookeepers]
    @@clusters[name] = self

    @zks = []
    @master = nil
    @slaves = []
    @ssh_input = []

    @state = "Initialized"
    sync
  end

  def ssh_input
    return @ssh_input
  end

  def status
    retval = {}
    retval['state'] = @state
    retval['num_zookeepers'] = @num_zookeepers
    retval['num_regionservers'] = @num_regionservers
    retval['launchTime'] = @launchTime
    retval['dnsName'] = @dnsName
    retval['name'] = @name
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
    zookeepers = 0
    @zks = []
    @slaves = []

    if !describe_instances.reservationSet
      #no instances yet (even terminated ones have been cleaned up)
      return self.status
    end

    describe_instances.reservationSet['item'].each do |ec2_instance_set|
      security_group = ec2_instance_set.groupSet['item'][0]['groupId']
      if (security_group == @name)
        slaves = ec2_instance_set['instancesSet']['item']
        slaves.each {|rs|
          if (rs['instanceState']['name'] != 'terminated')
            @slaves.push(rs)
          end
        }
      else
        if (security_group == (@name + "-zk"))
          zks = ec2_instance_set['instancesSet']['item']
          zks.each {|zk|
            if (zk['instanceState']['name'] != 'terminated')
              @zks.push(zk)
            end
          }
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

    if (@zks.size > 0)
      @num_zookeepers = @zks.size
    end

    if (@slaves.size > 0)
      @num_regionservers = @slaves.size
    end

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
    #..
    #      exec("~/hbase-ec2/bin/hbase-ec2 launch-cluster #{@name} #{@num_regionservers} #{@num_zookeepers}")
    init_hbase_cluster_secgroups
    launch_hbase_zookeepers
    launch_hbase_master
    launch_hbase_slaves
  end

  def init_hbase_cluster_secgroups
    # create security group @name, @name_master, and @name_slave
    groups = describe_security_groups
    found_master = false
    found_rs = false
    found_zk = false
    groups['securityGroupInfo']['item'].each { |group| 
      if group['groupName'] =~ /^#{@name}$/
        found_rs = true
      end
      if group['groupName'] =~ /^#{@name}-master$/
        found_master = true
      end
      if group['groupName'] =~ /^#{@name}-zk$/
        found_zk = true
      end
    }

    if (found_rs == false) 
      puts "creating new security group: #{@name}.."
      create_security_group({
        :group_name => "#{@name}",
        :group_description => "Group for HBase Slaves."
      })
      puts "..done"
    end

    if (found_master == false) 
      puts "creating new security group: #{@name}-master.."
      create_security_group({
        :group_name => "#{@name}-master",
        :group_description => "Group for HBase Master."
      })
      puts "..done"
    end

    if (found_zk == false) 
      puts "creating new security group: #{@name}-zk.."
      create_security_group({
        :group_name => "#{@name}-zk",
        :group_description => "Group for HBase Zookeeper quorum."
      })
      puts "..done"
    end
  end

  def launch_hbase_zookeepers
  end

  def launch_hbase_master
  end

  def launch_hbase_slaves
  end

  def hdfs_test(nrFiles=10,fileSize=1000)
    state = "begin"
    stderr = ""
    stdout = ""
    retval_hash = {}
    result_pairs = {}
    av_lines = []
    run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000",
             lambda{|line|
               stdout = stdout + line
               puts line
             },
             lambda{|line|
               stderr = stderr + line
               #implement finite state machine
               if line =~ /-+ TestDFSIO -+/
                 state = "results"
               end

               if state == "results"
                 av_lines.push(line)
               else
                 putc "."
               end
             })

    av_section = av_lines.join("\n")

    av_section.split(/\n/).each {|av_line|
      av_pair = av_line.split(/: /)
      if (av_pair[2])
        result_pairs[trim(av_pair[1])] = trim(av_pair[2])
      end
    }

    puts

    retval_hash['pairs'] = result_pairs
    retval_hash['stdout'] = stdout
    retval_hash['stderr'] = stderr

    retval_hash

  end

  def run_test(test,stdout_scanner = lambda{|line| puts line},stderr_scanner = lambda{|line| puts "(stderr): #{line}"})
    #fixme : fix hardwired version (first) then path to hadoop (later)
    ssh("/usr/local/hadoop-0.20-tm-2/bin/hadoop jar /usr/local/hadoop/hadoop-test-0.20-tm-2.jar #{test}",
        stdout_scanner,
        stderr_scanner)
  end

  def ssh(command,
          stdout_scanner = lambda{|line| puts line},
          stderr_scanner = lambda{|line| puts "(stderr): #{line}"})
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
        @ssh_input.push(command)
        channel.exec(command) do |ch, success|
          #FIXME: throw exception(?)
          puts "error: could not execute command '#{command}'" unless success
        end

        channel.on_data do |ch, data|
          stdout_scanner.call(data)
# example of how to talk back to server.
#          channel.send_data "something for stdin\n"
        end
        
        channel.on_extended_data do |ch, type, data|
          stderr_scanner.call(data)
        end
        
        channel.on_close do |ch|
          # cleanup, if any..
        end
      end
      
      channel.wait

    end
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



