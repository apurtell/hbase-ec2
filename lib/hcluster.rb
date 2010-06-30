#!/usr/bin/env ruby
require 'monitor'
require 'net/ssh'
require 'net/scp'
require 'AWS'

#FIXME: move to yaml config file.
EC2_ROOT_SSH_KEY = "#{ENV['HOME']}/.ec2/root.pem"

def trim(string = "")
  string.gsub(/^\s+/,'').gsub(/\s+$/,'')
end

class HClusterStateError < StandardError
end

class HClusterStartError < StandardError
end

class AWS::EC2::Base::HCluster < AWS::EC2::Base
  @@clusters = {}
  @@remote_init_script = "hbase-ec2-init-remote.sh"

#  @@default_base_ami_image = "ami-f61dfd9f"   # ec2-public-images/fedora-8-x86_64-base-v1.10.manifest.xml
  @@default_base_ami_image = "ami-70668e19"   # my trunk instance.
  @@m1_small_ami_image = "ami-48aa4921"       # ec2-public-images/fedora-8-i386-base-v1.10.manifest.xml
  @@c1_small_ami_image = "ami-48aa4921"       # ec2-public-images/fedora-8-i386-base-v1.10.manifest.xml

  attr_reader :zks, :master, :slaves, :aux, :zone, :zk_image_name, :master_image_name, :slave_image_name, :aux_image_name, :owner_id,:image_creator

  def initialize( name, options = {} )
    raise HClusterStartError, 
    "AMAZON_ACCESS_KEY_ID is not defined in your environment." unless ENV['AMAZON_ACCESS_KEY_ID']

    raise HClusterStartError, 
    "AMAZON_SECRET_ACCESS_KEY is not defined in your environment." unless ENV['AMAZON_SECRET_ACCESS_KEY']

    raise HClusterStartError,
    "AWS_ACCOUNT_ID is not defined in your environment." unless ENV['AWS_ACCOUNT_ID']
    # remove dashes so that describe_images() can find images owned by this owner.
    @owner_id = ENV['AWS_ACCOUNT_ID'].gsub(/-/,'')

    super(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])
    raise ArgumentError, 
    "HCluster name '#{name}' is already in use for cluster:\n#{@@clusters[name]}\n" if @@clusters[name]

    #architectures
#    @zk_arch = "i386"
    @zk_arch = "x86_64"
    @master_arch = "x86_64"
    @slave_arch = "x86_64"

    # image names below will work for your initial trials, but you
    # will want to change them to your own images.
    options = { 
      :num_regionservers => 5,
      :num_zookeepers => 1,
      :launch_aux => false,
      :zk_image_name => "hbase-0.21.0-SNAPSHOT-x86_64-ekoontz",
      :master_image_name => "hbase-0.21.0-SNAPSHOT-x86_64-ekoontz",
      :slave_image_name => "hbase-0.21.0-SNAPSHOT-x86_64-ekoontz",
      :debug_level => 0
    }.merge(options)

    @lock = Monitor.new
    
    @name = name
    @num_regionservers = options[:num_regionservers]
    @num_zookeepers = options[:num_zookeepers]
    @launch_aux = options[:launch_aux]
    @debug_level = options[:debug_level]

    @@clusters[name] = self

    @zks = []
    @master = nil
    @slaves = []
    @aux = nil
    @ssh_input = []

    @zone = "us-east-1a"

    #images
    @zk_image_name = options[:zk_image_name]
    @master_image_name = options[:master_image_name]
    @slave_image_name = options[:slave_image_name]

    #security_groups
    @zk_security_group = @name + "-zk"
    @rs_security_group = @name
    @master_security_group = @name + "-master"
    @aux_security_group = @name + "-aux"

    #machine instance types
#    @zk_instance_type = "m1.small"
    @zk_instance_type = "c1.xlarge"
    @rs_instance_type = "c1.xlarge"
    @master_instance_type = "c1.xlarge"

    #ssh keys
    @zk_key_name = "root"
    @rs_key_name = "root"
    @master_key_name = "root"

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
    if @aux
      retval['aux'] = @aux.instanceId
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

    describe_instances.reservationSet.item.each do |ec2_instance_set|
      security_group = ec2_instance_set.groupSet.item[0].groupId
      if (security_group == @name)
        slaves = ec2_instance_set.instancesSet.item
        slaves.each {|rs|
          if (rs.instanceState.name != 'terminated')
            @slaves.push(rs)
          end
        }
      else
        if (security_group == (@name + "-zk"))
          zks = ec2_instance_set.instancesSet.item
          zks.each {|zk|
            if (zk['instanceState']['name'] != 'terminated')
              @zks.push(zk)
            end
          }
        else
          if (security_group == (@name + "-master"))
            if ec2_instance_set.instancesSet.item[0].instanceState.name != 'terminated'
              @master = ec2_instance_set.instancesSet.item[0]
              @state = @master.instanceState.name
              @dnsName = @master.dnsName
              @launchTime = @master.launchTime
            end
          else
            if (security_group == (@name + "-aux"))
              if ec2_instance_set.instancesSet.item[0].instanceState.name != 'terminated'
                @aux = ec2_instance_set.instancesSet.item[0]
              end
            end
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

  def my_images
    describe_images({:owner_id => owner_id}).imagesSet.item.each {|image| puts "#{image.name}: #{image.imageId}"}
    nil
  end

  def create_image(options = {})
    options = {
      :hbase_version => "0.21.0-SNAPSHOT",
      :hadoop_version => "0.22.0-SNAPSHOT",
      :slave_instance_type => nil,
      :user => "ekoontz",
      :s3_bucket => "ekoontz-amis",
      :debug => false
    }.merge(options)

    #cleanup any existing create_image instances.
    if @image_creator
      terminate_instances({
                            :instance_id => @image_creator.instanceId
                          })
      @image_creator = nil
    end


    hbase_version = options[:hbase_version]
    hadoop_version = options[:hadoop_version]
    slave_instance_type = options[:slave_instance_type]
    user = options[:user]
    s3_bucket = options[:s3_bucket]
    #...
    # allow override of SLAVE_INSTANCE_TYPE from the command line 
    #[ ! -z $1 ] && SLAVE_INSTANCE_TYPE=$1
    if slave_instance_type == nil
      slave_instance_type = @rs_instance_type
    end
    
    type=slave_instance_type
    arch=@slave_arch
    
    image_name = "hbase-#{hbase_version}-#{arch}-#{user}"
    existing_image = describe_images({:owner_id => @owner_id}).imagesSet.item.detect {
      |image| image.name == image_name
    }
    
    if existing_image
      puts "Existing_image: #{existing_image.imageId} already registered for image name #{image_name}. Call deregister_image(:image_id => '#{existing_image.imageId}'), if desired."
      return existing_image.imageId
    end
    
    puts "Creating and registering image: #{image_name}"
    puts "Starting a AMI with ID: #{@@default_base_ami_image}."
    
    @image_creator = do_launch({
                                :image_id => @@default_base_ami_image,
                                :key_name => "root",
                                :instance_type => "m1.large"
                              },"image-creator")[0]
    

    image_creator_hostname = @image_creator.dnsName
    puts "Started image creator: #{image_creator_hostname}"

    puts "Copying scripts."
    until_ssh_able([@image_creator])
    
    scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/functions.sh","/mnt")
    scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/image/create-hbase-image-remote","/mnt")
    scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/image/ec2-run-user-data","/etc/init.d")
    
    # Copy private key and certificate (for bundling image)
    scp_to(image_creator_hostname,"#{ENV['HOME']}/.ec2/root.pem","/mnt")
    scp_to(image_creator_hostname,"#{ENV['HOME']}/.ec2/cert.pem","/mnt")
    
    puts "running create-hbase-image-remote on image builder: #{image_creator_hostname}; hbase_version=#{hbase_version}; hadoop_version=#{hadoop_version}.."
    hbase_url = "http://ekoontz-tarballs.s3.amazonaws.com/hbase-#{hbase_version}-bin.tar.gz"
    hadoop_url = "http://ekoontz-tarballs.s3.amazonaws.com/hadoop-common-#{hadoop_version}.tar.gz"
    lzo_url = "http://tm-files.s3.amazonaws.com/hadoop/lzo-linux-0.20-tm-2.tar.gz"
    java_url = "http://mlai.jdk.s3.amazonaws.com/jdk-6u20-linux-#{arch}.bin"

    puts("sh -c \"INSTANCE_TYPE=#{type} ARCH=#{arch} HBASE_VERSION=#{hbase_version} HADOOP_VERSION=#{hadoop_version} HBASE_URL=#{hbase_url} HADOOP_URL=#{hadoop_url} LZO_URL=#{lzo_url} JAVA_URL=#{java_url} AWS_ACCOUNT_ID=#{ENV['AWS_ACCOUNT_ID']} S3_BUCKET=#{options[:s3_bucket]} AWS_SECRET_ACCESS_KEY=#{ENV['AMAZON_SECRET_ACCESS_KEY']} AWS_ACCESS_KEY_ID=#{ENV['AMAZON_ACCESS_KEY_ID']} /mnt/create-hbase-image-remote\"")

    ssh_to(image_creator_hostname,
           "sh -c \"INSTANCE_TYPE=#{type} ARCH=#{arch} HBASE_VERSION=#{hbase_version} HADOOP_VERSION=#{hadoop_version} HBASE_URL=#{hbase_url} HADOOP_URL=#{hadoop_url} LZO_URL=#{lzo_url} JAVA_URL=#{java_url} AWS_ACCOUNT_ID=#{@owner_id} S3_BUCKET=#{options[:s3_bucket]} AWS_SECRET_ACCESS_KEY=#{ENV['AMAZON_SECRET_ACCESS_KEY']} AWS_ACCESS_KEY_ID=#{ENV['AMAZON_ACCESS_KEY_ID']} /mnt/create-hbase-image-remote\"",
           image_output_handler(options[:debug]))
    
    # Register image
    image_location = "#{s3_bucket}/hbase-#{hbase_version}-#{arch}.manifest.xml"

    puts "ec2-register -n #{image_name} #{image_location}"

    # FIXME: notify maintainers: 
    # http://amazon-ec2.rubyforge.org/AWS/EC2/Base.html#register_image-instance_method does not 
    # mention :name param (only :image_location).
    register_image({
                     :name => image_name,
                     :image_location => image_location
                   })
    
    puts "image registered."
    if (!(options[:debug] == true))
      puts "shutting down image-builder #{@image_creator.instanceId}"
      terminate_instances({
                            :instance_id => @image_creator.instanceId
                          })
      @image_creator = nil
    else
      puts "not shutting down image creator: #{@image_creator.dnsName}"
    end
    "(image name goes here)"
  end

  def image_output_handler(debug)
    #includes code to get past Sun/Oracle's JDK License consent prompts.
    lambda{|line,channel|
      if (debug == true)
        puts line
      end
      if line =~ /Do you agree to the above license terms/
        channel.send_data "yes\n"
      end
      if line =~ /Press Enter to continue/
        channel.send_data "\n"
      end
    }
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
    @state = "launching"

    init_hbase_cluster_secgroups
    launch_zookeepers
#    launch_master
#    launch_slaves
#    if @launch_aux
#      launch_aux
#    end

    # if threaded, we would set to "pending" and then 
    # use join to determine when state should transition to "running".
    @launchTime = master.launchTime
    @state = "running"
  end

  def init_hbase_cluster_secgroups
    # create security group @name, @name_master, and @name_slave
    groups = describe_security_groups
    found_master = false
    found_rs = false
    found_zk = false
    found_aux = false
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
      if group['groupName'] =~ /^#{@name}-aux$/
        found_aux = true
      end
    }

    if (found_aux == false) 
      puts "creating new security group: #{@name}-aux.."
      create_security_group({
        :group_name => "#{@name}-aux",
        :group_description => "Group for HBase Auxiliaries."
      })
    end

    if (found_rs == false) 
      puts "creating new security group: #{@name}.."
      create_security_group({
        :group_name => "#{@name}",
        :group_description => "Group for HBase Slaves."
      })
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

    # allow ssh from each..
    ["#{@name}","#{@name}-aux","#{@name}-master","#{@name}-zk"].each {|group|
      begin
        authorize_security_group_ingress(
                                         {
                                           :group_name => group,
                                           :from_port => 22,
                                           :to_port => 22,
                                           :cidr_ip => "0.0.0.0/0",
                                           :ip_protocol => "tcp"
                                         }
                                         )
      rescue AWS::InvalidPermissionDuplicate
        # authorization already exists - no problem.
      end

      #reciprocal access for each security group.
      ["#{@name}","#{@name}-aux","#{@name}-master","#{@name}-zk"].each {|other_group|
        if (group != other_group)
          begin
            authorize_security_group_ingress(
                                             {
                                               :group_name => group,
                                               :source_security_group_name => other_group
                                             }
                                             )
          rescue AWS::InvalidPermissionDuplicate
            # authorization already exists - no problem.
          end
        end
      }
    }

  end

  def do_launch(options,name="",on_boot = nil)
    instances = run_instances(options)
    watch(name,instances)
    if on_boot
      on_boot.call(instances.instancesSet.item)
    end
    return instances.instancesSet.item
  end

  def watch(name,instances,begin_output = "[launch:#{name}",end_output = "]\n")
    # note: this aws_connection is separate for this watch() function call:
    # this will hopefully allow us to run watch() in a separate thread if desired.
    aws_connection = AWS::EC2::Base.new(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])

    print begin_output
    STDOUT.flush

    wait = true
    until wait == false
      wait = false
      instances.instancesSet.item.each_index {|i| 
        instance = instances.instancesSet.item[i]
        # get status of instance instance.instanceId.
        begin
          instance_info = aws_connection.describe_instances({:instance_id => instance.instanceId}).reservationSet.item[0].instancesSet.item[0]
          status = instance_info.instanceState.name
          if (!(status == "running"))
            wait = true
          else
            #instance is running 
            if @debug_level > 0
              puts "watch(#{name}): #{instance.instanceId} : #{status}"
            end
            instances.instancesSet.item[i] = instance_info
          end
        rescue AWS::InvalidInstanceIDNotFound
          wait = true
          puts " watch(#{name}): instance '#{instance.instanceId}' not found (might be transitory problem; retrying.)"
        end
      }
      if wait == true
        putc "."
        sleep 1
      end
    end

    print end_output
    STDOUT.flush

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
    options[:availability_zone] = @zone
    @zks = do_launch(options,"zk",lambda{|zks|setup_zookeepers(zks)})
  end

  def setup_zookeepers(zks)
    #when zookeepers are ready, copy info over to them..
    #for each zookeeper, copy ~/hbase-ec2/bin/hbase-ec2-init-zookeeper-remote.sh to zookeeper, and run it.
    until_ssh_able(zks)
    zks.each {|zk|
      if (@debug_level > 0)
        puts "zk dnsname: #{zk.dnsName}"
      end
      scp_to(zk.dnsName,File.dirname(__FILE__) +"/../bin/hbase-ec2-init-zookeeper-remote.sh","/var/tmp")
      ssh_to(zk.dnsName,
             "sh -c \"ZOOKEEPER_QUORUM=\\\"#{zookeeper_quorum}\\\" sh /var/tmp/hbase-ec2-init-zookeeper-remote.sh\"",
             echo_stdout,echo_stderr,
             "[setup:zk:#{zk.dnsName}",
             "]\n")
    }
  end

  def zookeeper_quorum
    retval = ""
    @zks.each {|zk|
      retval = "#{retval} #{zk.privateDnsName}"
    }
    trim(retval)
  end

  def launch_master
    options = {}
    options[:image_id] = master_image['imageId'] 
    options[:min_count] = 1
    options[:max_count] = 1
    options[:security_group] = @master_security_group
    options[:instance_type] = @master_instance_type
    options[:key_name] = @master_key_name
    options[:availability_zone] = @zone
    @master = do_launch(options,"master",lambda{|instances| setup_master(instances[0])})[0]
  end
  
  def launch_slaves
    options = {}
    options[:image_id] = regionserver_image['imageId']
    options[:min_count] = @num_regionservers
    options[:max_count] = @num_regionservers
    options[:security_group] = @rs_security_group
    options[:instance_type] = @rs_instance_type
    options[:key_name] = @rs_key_name
    options[:availability_zone] = @zone
    @slaves = do_launch(options,"rs",lambda{|instances|setup_slaves(instances)})
  end

  def launch_aux
    options = {}
    options[:image_id] = regionserver_image['imageId']
    options[:min_count] = 1
    options[:max_count] = 1
    options[:security_group] = @aux_security_group
    options[:instance_type] = @rs_instance_type
    options[:key_name] = @rs_key_name
    options[:availability_zone] = @zone
    @aux = do_launch(options,"aux",lambda{|instances|setup_aux(instances[0])})[0]
  end

  def setup_master(master)
    #cluster's dnsName is same as master's.
    @dnsName = master.dnsName
    @master = master

    until_ssh_able([master])

    @master.state = "running"
    # <ssh key>
    scp_to(master.dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
    #FIXME: should be 400 probably.
    ssh_to(master.dnsName,"chmod 600 /root/.ssh/id_rsa",consume_output,consume_output,nil,nil)
    # </ssh key>
        
    # <master init script>
    init_script = File.dirname(__FILE__) +"/../bin/#{@@remote_init_script}"
    scp_to(master.dnsName,init_script,"/root/#{@@remote_init_script}")
    ssh_to(master.dnsName,"chmod 700 /root/#{@@remote_init_script}",consume_output,consume_output,nil,nil)
    # NOTE : needs zookeeper quorum: requires zookeeper to have come up.
    ssh_to(master.dnsName,"sh /root/#{@@remote_init_script} #{master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
           summarize_output,summarize_output,"[setup:master:#{master.dnsName}","]\n")
  end

  def setup_slaves(slaves) 
    init_script = File.dirname(__FILE__) +"/../bin/#{@@remote_init_script}"
    #FIXME: requires that both master (master.dnsName) and zookeeper (zookeeper_quorum) to have come up.
    until_ssh_able(slaves)
    slaves.each {|slave|
      # <ssh key>
      scp_to(slave.dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
      #FIXME: should be 400 probably.
      ssh_to(slave.dnsName,"chmod 600 /root/.ssh/id_rsa",consume_output,consume_output,nil,nil)
      # </ssh key>

      scp_to(slave.dnsName,init_script,"/root/#{@@remote_init_script}")
      ssh_to(slave.dnsName,"chmod 700 /root/#{@@remote_init_script}",consume_output,consume_output,nil,nil)
      ssh_to(slave.dnsName,"sh /root/#{@@remote_init_script} #{@master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
             summarize_output,summarize_output,"[setup:rs:#{slave.dnsName}","]\n")
    }
  end

  def setup_aux(aux) 
    #NOTE:if setup process is multithreaded, setup_aux requires 
    # master.dnsName and zookeeper_quorum to be known.
    until_ssh_able([aux])
    dnsName = aux.dnsName

    # <ssh key>
    scp_to(dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
    #FIXME: should be 400 probably.
    ssh_to(dnsName,"chmod 600 /root/.ssh/id_rsa",consume_output,consume_output,nil,nil)
    # </ssh key>

    init_script = "#{ENV['HOME']}/hbase-ec2/bin/#{@@remote_init_script}"
    scp_to(dnsName,init_script,"/root/#{@@remote_init_script}")
    ssh_to(dnsName,"chmod 700 /root/#{@@remote_init_script}",consume_output,consume_output,nil,nil)
    ssh_to(dnsName,"sh /root/#{@@remote_init_script} #{@master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
           summarize_output,summarize_output,"[setup:aux:#{dnsName}","]\n")
  end

  def terminate_zookeepers
    @zks.each { |zk|
      options = {}
      if zk.instanceId
        options[:instance_id] = zk.instanceId
        puts "terminating zookeeper: #{zk.instanceId}"
        terminate_instances(options)
      end
    }
    @zks = []
  end

  def terminate_master
    if @master && @master.instanceId
      options = {}
      options[:instance_id] = @master.instanceId
      puts "terminating master: #{@master.instanceId}"
      terminate_instances(options)
    end
    @master = nil
  end

  def terminate_slaves
    @slaves.each { |slave|
      if slave.instanceId
        options = {}
        options[:instance_id] = slave.instanceId
        puts "terminating regionserver: #{slave.instanceId}"
        terminate_instances(options)
      end
    }
    @slaves = []
  end

  def terminate_aux
    if @aux && @aux.instanceId
      options = {}
      options[:instance_id] = @aux.instanceId
      puts "terminating auxiliary: #{@aux.instanceId}"
      terminate_instances(options)
    end
    @aux = nil
  end

  def terminate_image_creator
    if @image_creator && @image_creator['instanceId']
      terminate_instances({:instance_id => @image_creator['instanceId']})
    end
  end

  def describe_instances(options = {}) 
    retval = nil
    @lock.synchronize {
      retval = super(options)
    }
    retval
  end

  def zk_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    if_null_image(
                  describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
                    |image| image['name'] == @zk_image_name
                  },@zk_image_name)
  end

  def regionserver_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    if_null_image(
                  describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
                    |image| image['name'] == @slave_image_name
                  },@regionserver_image)
  end

  def master_image
    #specifying owner_id speeds up describe_images() a lot, but only works if the image is owned by @owner.
    if_null_image(
                  describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
                    |image| image['name'] == @master_image_name
                  },@master_image_name)
  end

  def if_null_image(retval,image_name)
    if !retval
      # try default image instead.
      retval = describe_images({:owner_id => @owner_id})['imagesSet']['item'].detect{
        |image| image['imageId'] == @@default_base_ami_image
      }
      if !retval
        raise HClusterStartError, 
        "Could not find image '#{image_name}' in instances owned by AWS Account ID: '#{@owner_id}'."
      else
        "Warning: could not find image '#{image_name}; using default base ami image #{@@default_base_ami_image} instead."
      end
    end
    retval
  end

  def run_test(test,stdout_line_reader = lambda{|line,channel| puts line},stderr_line_reader = lambda{|line| puts "(stderr): #{line}"})
    #fixme : fix hardwired version (first) then path to hadoop (later)
    ssh("/usr/local/hadoop-0.20-tm-2/bin/hadoop jar /usr/local/hadoop-0.20-tm-2/hadoop-test-0.20-tm-2.jar #{test}",
        stdout_line_reader,
        stderr_line_reader)
  end

  def ssh_to(host,command,
             stdout_line_reader = lambda{|line,channel| puts line},
             stderr_line_reader = lambda{|line| puts "(stderr): #{line}"},
             begin_output = nil,
             end_output = nil)
    # variant of ssh with different param ordering.
    ssh(command,stdout_line_reader,stderr_line_reader,host,begin_output,end_output)
  end

  # send a command and handle stdout and stderr 
  # with supplied anonymous functions (puts by default)
  # to a specific host (master by default).
  def ssh(command,
          stdout_line_reader = echo_stdout,
          stderr_line_reader = echo_stderr,
          host = self.master.dnsName,
          begin_output = nil,
          end_output = nil)
#    # FIXME: if self.state is not running, then allow queuing of ssh commands, if desired.
    if (host == @dnsName)
      raise HClusterStateError,
      "HCluster '#{@name}' has no master hostname. Cluster summary:\n#{self.to_s}\n" if (host == nil)
    end

    if begin_output
      print begin_output
      STDOUT.flush
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
          stdout_line_reader.call(data,channel)
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
    if end_output
      print end_output
      STDOUT.flush
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

  def terminate
    terminate_zookeepers
    terminate_master
    terminate_slaves
    terminate_aux
    @state = "terminated"
    status
  end
  
  def to_s
    retval = "HCluster '#{@name}' (state='#{@state}'): #{@num_regionservers} regionserver#{((@numregionservers == 1) && '') || 's'}; #{@num_zookeepers} zookeeper#{((@num_zookeepers == 1) && '') || 's'}"
    if (@aux) 
      retval = retval + "; 1 aux"
    end
    retval = retval + "."
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

  def until_ssh_able(instances)
    instances.each {|instance|
      connected = false
      until connected == true
        begin
          ssh_to(instance.dnsName,"true",consume_output,consume_output,nil,nil)
          connected = true
        rescue Net::SSH::AuthenticationFailed
          if @debug_level > 0
            puts "host: #{instance.dnsName} not ready yet - waiting.."
          end
          sleep 5
        rescue Errno::ECONNREFUSED
          if @debug_level > 0
            puts "host: #{instance.dnsName} not ready yet - waiting.."
          end
          sleep 5
        rescue Errno::ETIMEDOUT
          if @debug_level > 0
            puts "host: #{instance.dnsName} not ready yet - waiting.."
          end
          sleep 5
        end
      end
    }
  end

  def echo_stdout
    return lambda{|line|
      puts line
    }
  end

  def echo_stderr 
    return lambda{|line|
      puts "(stderr): #{line}"
    }
  end

  def consume_output 
    #don't print anything for each line.
    return lambda{|line|
    }
  end

  def summarize_output
    #output one '.' per line.
    return lambda{|line,channel|
      putc "."
    }
  end

end



