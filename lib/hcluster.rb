#!/usr/bin/env ruby
require 'AWS'
require 'net/ssh'
require 'net/scp'

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

    #architectures
    @zk_arch = "i386"
    @master_arch = "x86_64"
    @slave_arch = "x86_64"

    #images
    @zk_image_name = "hbase-0.20-tm-2-#{@zk_arch}-ekoontz"
    @master_image_name = "hbase-0.20-tm-2-#{@master_arch}-ekoontz"
    @slave_image_name = "hbase-0.20-tm-2-#{@slave_arch}-ekoontz"

    #security_groups
    @zk_security_group = @name + "-zk"
    @rs_security_group = @name
    @master_security_group = @name + "-master"

    #machine instance types
    @zk_instance_type = "m1.small"
    @rs_instance_type = "c1.xlarge"
    @master_instance_type = "c1.xlarge"

    #ssh keys
    @zk_key_name = "root"
    @rs_key_name = "root"
    @master_key_name = "root"

    @owner_id = "155698749257"

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

  def launch
    #kill existing 'launch' threads for this cluster, if any.
    #..
    init_hbase_cluster_secgroups
    launch_zookeepers
    launch_master
    launch_slaves
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

  def zks
    @zks
  end

  def launch_zookeepers
    options = {}
    zk_img_id = zk_image['imageId']
    options[:image_id] = zk_img_id
    options[:min_count] = @num_zookeepers
    options[:max_count] = @num_zookeepers
    options[:security_group] = @zk_security_group
    options[:instance_type] = @zk_instance_type
    options[:key_name] = @zk_key_name
    puts "starting zookeepers.."
    @zks = supervised_launch(options)
    #fix me: test for ssh-ability rather than sleeping
    sleep 10

    setup_zookeepers
  end

  def setup_zookeepers
    #when zookeepers are ready, copy info over to them..
    #for each zookeeper, copy ~/hbase-ec2/bin/hbase-ec2-init-zookeeper-remote.sh to zookeeper, and run it.

    @zks.instancesSet.item.each {|zk|
      scp_to(zk.dnsName,"#{ENV['HOME']}/hbase-ec2/bin/hbase-ec2-init-zookeeper-remote.sh","/var/tmp")
      ssh_to(zk.dnsName,"sh -c \"ZOOKEEPER_QUORUM=\\\"#{zookeeper_quorum}\\\" sh /var/tmp/hbase-ec2-init-zookeeper-remote.sh\"")
    }
  end

  def zookeeper_quorum
    retval = ""
    @zks.instancesSet.item.each {|zk|
      retval = "#{retval} #{zk.privateDnsName}"
    }
    trim(retval)
  end

  def terminate_zookeepers
    @zks.instancesSet.item.each { |zk|
      options = {}
      options[:instance_id] = zk.instanceId
      puts "terminating zookeeper: #{zk.instanceId}"
      terminate_instances(options)
    }
  end

  def terminate_slaves
    if @slaves.size > 0 
      if @slaves.instancesSet
        @slaves.instancesSet.item.each { |slave|
          options = {}
          options[:instance_id] = slave.instanceId
          puts "terminating regionserver: #{slave.instanceId}"
          terminate_instances(options)
        }
      end
    end
  end

  def terminate_master
    options = {}
    options[:instance_id] = @master.instanceId
    puts "terminating master: #{@master.instanceId}"
    terminate_instances(options)
  end

  def launch_master
    options = {}
    master_img_id = master_image['imageId']
    options[:image_id] = master_img_id
    options[:min_count] = 1
    options[:max_count] = 1
    options[:security_group] = @master_security_group
    options[:instance_type] = @master_instance_type
    options[:key_name] = @master_key_name

    #only one master, but we'll use an array called "@master_instances" because
    #run_instances() returns an array.
    puts "starting master.."

    @master_instances = supervised_launch(options)

    @master = @master_instances.instancesSet.item[0]
    @zone = @master.placement.availabilityZone
    #fix me: test for ssh-ability rather than sleeping
    sleep 10

    scp_to(@master.dnsName,"#{ENV['HOME']}/.ec2/root.pem","/root/.ssh/id_rsa")
    #FIXME: should be 400 probably.
    ssh_to(@master.dnsName,"chmod 600 /root/.ssh/id_rsa")
  end

  def launch_slaves
    options = {}
    rs_img_id = regionserver_image['imageId']
    options[:image_id] = rs_img_id
    options[:min_count] = @num_regionservers
    options[:max_count] = @num_regionservers
    options[:security_group] = @rs_security_group
    options[:instance_type] = @rs_instance_type
    options[:key_name] = @rs_key_name
    options[:availability_zone] = @zone
    puts "starting regionservers.."
    @slaves = supervised_launch(options)
  end

  def supervised_launch(options)
    #<thread>
    instances = run_instances(options)

    #FIXME: add support for ENABLE_ELASTIC_IPS (see launch-hbase-zookeeper.)
    wait = true
    until wait == false
      wait = false
      instances.instancesSet.item.each_index {|i| 
        instance = instances.instancesSet.item[i]
        # get status of instance instance.instanceId.
        instance_info = describe_instances({:instance_id => instance.instanceId}).reservationSet.item[0].instancesSet.item[0]
        status = instance_info.instanceState.name
        puts "supervised_launch: #{instance.instanceId} : #{status}"
        if (!(status == "running"))
          wait = true
        else
          #instance is running 
          instances.instancesSet.item[i] = instance_info
        end
        if wait == true
          sleep 10
        end
      }
    end
    return instances
  end

  def zk_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
      |image| image['name'] == @zk_image_name
    }
  end

  def regionserver_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
      |image| image['name'] == @slave_image_name
    }
  end

  def master_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
      |image| image['name'] == @master_image_name
    }
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

  def run_test(test,stdout_line_reader = lambda{|line| puts line},stderr_line_reader = lambda{|line| puts "(stderr): #{line}"})
    #fixme : fix hardwired version (first) then path to hadoop (later)
    ssh("/usr/local/hadoop-0.20-tm-2/bin/hadoop jar /usr/local/hadoop/hadoop-test-0.20-tm-2.jar #{test}",
        stdout_line_reader,
        stderr_line_reader)
  end

  def ssh_to(host,command,
             stdout_line_reader = lambda{|line| puts line},
             stderr_line_reader = lambda{|line| puts "(stderr): #{line}"})
    # variant of ssh with different param ordering.
    ssh(command,stdout_line_reader,stderr_line_reader,host)
  end

  # send a command and handle stdout and stderr 
  # with supplied anonymous functions (puts by default)
  # to a specific host (master by default).
  def ssh(command,
          stdout_line_reader = lambda{|line| puts line},
          stderr_line_reader = lambda{|line| puts "(stderr): #{line}"},
          host = @dnsName)
    # FIXME: if self.state is not running, then allow queuing of ssh commands, if desired.

    if (host == @dnsName)
      raise HClusterStateError,
      "HCluster '#{name}' is not in running state:\n#{self.to_s}\n" if @state != 'running'
    end
    # http://net-ssh.rubyforge.org/ssh/v2/api/classes/Net/SSH.html#M000013
    # paranoid=>false because we should ignore known_hosts, since AWS IPs get frequently recycled
    # and their servers' private keys will vary.
    Net::SSH.start(host,'root',
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
          stdout_line_reader.call(data)
# example of how to talk back to server.
#          channel.send_data "something for stdin\n"
        end
        
        channel.on_extended_data do |ch, type, data|
          stderr_line_reader.call(data)
        end
        
        channel.on_close do |ch|
          # cleanup, if any..
        end
      end
      
      channel.wait

    end
  end

  def scp_to(host,local_path,remote_path)
    #http://net-ssh.rubyforge.org/scp/v1/api/classes/Net/SCP.html#M000005
    # paranoid=>false because we should ignore known_hosts, since AWS IPs get frequently recycled
    # and their servers' private keys will vary.
    Net::SCP.start(host,'root',
                   :keys => ["~/.ec2/root.pem"],
                   :paranoid => false
                   ) do |scp|
      scp.upload! local_path,remote_path
    end
  end

  def master
    @master
  end

  def slaves
    @slaves
  end

  def terminate
    terminate_zookeepers
    terminate_master
    terminate_slaves
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



