#!/usr/bin/env ruby
require 'monitor'
require 'net/ssh'
require 'net/scp'
require 'AWS'
require 'aws/s3'

module Hadoop

  class Himage < AWS::EC2::Base
    attr_reader :label,:image_id,:image,:shared_base_object, :owner_id

    @@owner_id = ENV['AWS_ACCOUNT_ID'].gsub(/-/,'')

    begin
      @@shared_base_object = AWS::EC2::Base.new({
                                                  :access_key_id => ENV['AMAZON_ACCESS_KEY_ID'],
                                                  :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
                                                })
    rescue
      puts "ooops..maybe you didn't define AMAZON_ACCESS_KEY_ID or AMAZON_SECRET_ACCESS_KEY? "
    end

    @@s3 = AWS::S3::S3Object

    if !(@@s3.connected?)
      @@s3.establish_connection!(
                                 :access_key_id => ENV['AMAZON_ACCESS_KEY_ID'],
                                 :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
                                 )
    end

    def list
      HCluster.my_images
    end

    def Himage::s3
      @@s3
    end

    def Himage::upload_tar(label = "test", bucket = "ekoontz-tarballs",file="/Users/ekoontz/s3/sample.tar.gz")
      filename = File.basename(file)
      puts "storing '#{filename}' in s3 bucket '#{bucket}'.."
      @@s3.store filename, open(file), bucket,:access => :public_read
      puts "done."
    end

    def Himage::list
      HCluster.my_images
    end

    def initialize_himage_usage
      puts ""
      puts "Himage.new"
      puts "  options: (default)"
      puts "   :label  (nil) (see HImage.list for a list of labels)"
      puts ""
      puts "Himage.list shows a list of possible :label values."
    end

    def initialize(options = {})
      @shared_base_object = @@shared_base_object
      @owner_id = @@owner_id
      options = {
        :owner_id => @@owner_id
      }.merge(options)

      if options[:label]
        image_label = options[:label]
        owned_image = Himage::find_owned_image(options)
        if owned_image
          @image = owned_image
          owned_image
        else
          retval = HCluster.create_image(options)
          puts "image id (retval of HCluster.create_image(#{options.to_yaml})): #{retval}"
          @image = Himage::find_owned_image(options)
        end
        @label = @image.name
        @image_id = @image.imageId
        @image
      else
        #not enough options: show usage and exit.
        initialize_himage_usage
      end
    end

    def Himage.find_owned_image(options)
      options = {
        :owner_id => @@owner_id
        }.merge(options)
      return Himage.describe_images(options,false)
    end

    def describe_images(options = {})
      options = {
        :owner_id => @@owner_id
        }.merge(options)
      return Himage.describe_images(options,false)
    end

    def Himage.describe_images(options = {},search_all_visible_images = true)
      image_label = options[:label]
      if image_label
        options = {
          :owner_id => @@owner_id
        }.merge(options)

        retval = @@shared_base_object.describe_images(options)
        #filter by image_label
        if image_label
          retval2 = retval['imagesSet']['item'].detect{
            |image| image['name'] == image_label
          }
        else
          retval2 = retval['imagesSet']['item'].detect{
            |image| image['image_id'] == options[:image_id]
          }
        end
        
        if (retval2 == nil and search_all_visible_images == true)
          options.delete(:owner_id)
          puts "image '#{image_label}' not found in owner #{@@owner_id}'s images; looking in all images (may take a while..)"
          retval = @@shared_base_object.describe_images(options)
          #filter by image_label
          retval2 = retval['imagesSet']['item'].detect{
            |image| image['name'] == image_label
          }
        end
        retval2
      else
        @@shared_base_object.describe_images(options)
      end
    end

    def deregister
      Himage.deregister(self.image.imageId)
    end

    def Himage.deregister(image)
      @@shared_base_object.deregister_image({:image_id => image})
    end


  end
  
  #FIXME: move to yaml config file.
  EC2_ROOT_SSH_KEY = "#{ENV['HOME']}/.ec2/root.pem"
    
  class HClusterStateError < StandardError
  end
  
  class HClusterStartError < StandardError
  end
  
  class HCluster < AWS::EC2::Base

    def trim(string = "")
      string.gsub(/^\s+/,'').gsub(/\s+$/,'')
    end
    
    @@clusters = []
    @@remote_init_script = "hbase-ec2-init-remote.sh"
    
    # used for creating hbase images.
    @@default_base_ami_image = "ami-f61dfd9f"   # ec2-public-images/fedora-8-x86_64-base-v1.10.manifest.xml
    @@owner_id = ENV['AWS_ACCOUNT_ID'].gsub(/-/,'')
    
    #architectures: either "x86_64" or "i386".
    @@zk_arch = "x86_64"
    @@master_arch = "x86_64"
    @@slave_arch = "x86_64"
    
    @@debug_level = 0
    
    # I feel like the describe_images method should be a class,
    # not, as in AWS::EC2::Base, an object method,
    # so I use this in HCluster::describe_images.
    # This is used to look up images, and is read-only, (except for a usage of AWS::EC2::Base::register_image below)
    # so hopefully, no race conditions are possible.
    begin
      @@shared_base_object = AWS::EC2::Base.new({
                                                  :access_key_id => ENV['AMAZON_ACCESS_KEY_ID'],
                                                  :secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY']
                                                })
    rescue
      puts "ooops..maybe you didn't define AMAZON_ACCESS_KEY_ID or AMAZON_SECRET_ACCESS_KEY? "
  end
    
    attr_reader :zks, :master, :slaves, :aux, :zone, :zk_image_label,
    :master_image_label, :slave_image_label, :aux_image_label, :owner_id,
    :image_creator,:options,:hbase_version
    
    def initialize_print_usage
      puts ""
      puts "HCluster.new"
      puts "  options: (default)"
      puts "   :label (nil) (see HCluster.my_images for a list of labels)"
      puts "   :hbase_version (ENV['HBASE_VERSION'])"
      puts "   :num_regionservers  (3)"
      puts "   :num_zookeepers  (1)"
      puts "   :launch_aux  (false)"
      puts "   :zk_arch  (x86_64)"
      puts "   :master_arch  (x86_64)"
      puts "   :slave_arch  (x86_64)"
      puts "   :debug_level  (@@debug_level)"
      puts "   :validate_images  (true)"
      puts "   :security_group_prefix (hcluster)"
      puts ""
      puts "HCluster.my_images shows a list of possible :label values."
    end

    def initialize( options = {} )

      if options.size == 0
        #not enough info to create cluster: show documentation.
        initialize_print_usage
        return nil
      end

      options = {
        :label => nil,
        :hbase_version => ENV['HBASE_VERSION'],
        :num_regionservers => 3,
        :num_zookeepers => 1,
        :launch_aux => false,
        :zk_arch => "x86_64",
        :master_arch => "x86_64",
        :slave_arch => "x86_64",
        :debug_level => @@debug_level,
        :validate_images => true,
        :security_group_prefix => "hcluster",
      }.merge(options)
      
      # using same security group for all instances does not work now, so forcing to be separate.
      options[:separate_security_groups] = true

      if options[:label]
        options = {
          :zk_image_label => options[:label],
          :master_image_label => options[:label],
          :slave_image_label => options[:label]
        }.merge(options)
      else
        if options[:hbase_version]
          options = {
            :zk_image_label => "hbase-#{options[:hbase_version]}-#{options[:zk_arch]}",
            :master_image_label => "hbase-#{options[:hbase_version]}-#{options[:master_arch]}",
            :slave_image_label => "hbase-#{options[:hbase_version]}-#{options[:slave_arch]}",
          }.merge(options)
        else
          # User has no HBASE_VERSION defined, so check my images and use the first one.
          # If possible, would like to apply further filtering to find suitable images amongst 
          # them rather than just picking first.
          desc_images = HCluster.describe_images({:owner_id => @@owner_id})
          if desc_images
            desc_images = desc_images.imagesSet.item
            if desc_images[0] && desc_images[0].name
              puts "No HBASE_VERSION defined in your environment: using #{desc_images[0].name}."
              options = {
                :zk_image_label => desc_images[0].name,
                :master_image_label => desc_images[0].name,
                :slave_image_label => desc_images[0].name
              }.merge(options)
            else
              raise HClusterStartError,"No suitable HBase images found in your AMI list. Please create at least one with create_image()."
            end
          else
            raise HClusterStartError,"No suitable HBase images found in your AMI list. Please create at least one with create_image()."
          end
        end

      end
            
      # check env variables.
      raise HClusterStartError, 
      "AMAZON_ACCESS_KEY_ID is not defined in your environment." unless ENV['AMAZON_ACCESS_KEY_ID']
      
      raise HClusterStartError, 
      "AMAZON_SECRET_ACCESS_KEY is not defined in your environment." unless ENV['AMAZON_SECRET_ACCESS_KEY']
      
      raise HClusterStartError,
      "AWS_ACCOUNT_ID is not defined in your environment." unless ENV['AWS_ACCOUNT_ID']
      # remove dashes so that describe_images() can find images owned by this owner.
      @@owner_id = ENV['AWS_ACCOUNT_ID'].gsub(/-/,'')
      
      super(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])
      
      #architectures: either "x86_64" or "i386".
      @zk_arch = "x86_64"
      @master_arch = "x86_64"
      @slave_arch = "x86_64"
      
      #for debugging
      @options = options
      @owner_id = @@owner_id
      
      @lock = Monitor.new
      
      @num_regionservers = options[:num_regionservers]
      @num_zookeepers = options[:num_zookeepers]
      @launch_aux = options[:launch_aux]
      @debug_level = options[:debug_level]
      
      @@clusters.push self
      
      @zks = []
      @master = nil
      @slaves = []
      @aux = nil
      @ssh_input = []
      
      @zone = "us-east-1a"
      
      #images
      @zk_image_label = options[:zk_image_label]
      @master_image_label = options[:master_image_label]
      @slave_image_label = options[:slave_image_label]
      
      if (options[:validate_images] == true)
        #validate image names (make sure they exist in Amazon's set).
        @zk_image_ = zk_image
        if (!@zk_image_)
          raise HClusterStartError,
          "could not find image called '#{@zk_image_label}'."
        end
        
        @master_image_ = master_image
        if (!@master_image_)
          raise HClusterStartError,
          "could not find image called '#{@master_image_label}'."
        end
        
        @slave_image_ = regionserver_image
        if (!@slave_image_)
          raise HClusterStartError,
          "could not find image called '#{@slave_image_label}'."
        end
        
      end
      
      #security_groups
      @security_group_prefix = options[:security_group_prefix]
      if (options[:separate_security_groups] == true)
        @zk_security_group = @security_group_prefix + "-zk"
        @rs_security_group = @security_group_prefix
        @master_security_group = @security_group_prefix + "-master"
        if options[:launch_aux] == true
          @aux_security_group = @security_group_prefix + "-aux"
        end
      else
        @zk_security_group = @security_group_prefix
        @rs_security_group = @security_group_prefix
        @master_security_group = @security_group_prefix
        if options[:launch_aux] == true
          @aux_security_group = @security_group_prefix
        end
      end
      
      #machine instance types
      #    @zk_instance_type = "m1.small"
      #    @zk_instance_type = "c1.xlarge"
      #    @rs_instance_type = "c1.xlarge"
      #    @master_instance_type = "c1.xlarge"
      @zk_instance_type = "m1.large"
      @rs_instance_type = "m1.large"
      @master_instance_type = "m1.large"
      
      #ssh keys
      @zk_key_name = "root"
      @rs_key_name = "root"
      @master_key_name = "root"
      
      @state = "Initialized"
      
      sync
    end
    
    def dnsName
      master.dnsName
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
      # where security_group = @security_group_prefix
      
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
        if (security_group == @security_group_prefix)
        slaves = ec2_instance_set.instancesSet.item
          slaves.each {|rs|
            if (rs.instanceState.name != 'terminated')
              @slaves.push(rs)
            end
          }
        else
          if (security_group == (@security_group_prefix + "-zk"))
            zks = ec2_instance_set.instancesSet.item
            zks.each {|zk|
              if (zk['instanceState']['name'] != 'terminated')
                @zks.push(zk)
              end
          }
          else
            if (security_group == (@security_group_prefix + "-master"))
              if ec2_instance_set.instancesSet.item[0].instanceState.name != 'terminated'
                @master = ec2_instance_set.instancesSet.item[0]
              @state = @master.instanceState.name
                @dnsName = @master.dnsName
                @launchTime = @master.launchTime
              end
            else
              if (security_group == (@security_group_prefix + "-aux"))
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
      HCluster.my_images
    end

    def HCluster.my_images
      #FIXME: figure out fixed width/truncation for pretty printing tables.
      puts "Label\t\t\t\tAMI"
      puts "=========================================="
      describe_images({:owner_id => @@owner_id}).imagesSet.item.each {|image| 
        puts "#{image.name}\t\t#{image.imageId}"
      }
      nil
    end
    
    def HCluster.deregister_image(image)
      @@shared_base_object.deregister_image({:image_id => image})
    end

    def HCluster.create_image_print_usage
      puts ""
      puts "HCluster.create_image"
      puts "  options: (default)"
      puts "  :label (nil) (see HCluster.my_images for a list of labels)"
      puts "  :hbase_version (ENV['HBASE_VERSION'])"
      puts "  :hadoop_version (ENV['HADOOP_VERSION'])"
      puts "  :slave_instance_type (nil)"
      puts "  :debug (false)"
#FIXME: use ENV as above.
      puts "  :user (ekoontz)"
      puts "  :s3_bucket (ekoontz-amis)"
      puts ""
      puts "HCluster.my_images shows a list of possible :label values."
    end
    
    def HCluster.create_image(options = {})
      if options.size == 0
        return create_image_print_usage
      end

      if options[:label]
        options = {
          :hbase_version => label_to_hbase_version(options[:label])
        }.merge(options)
      end

      options = {
        :label => nil,
        :hbase_version => "#{ENV['HBASE_VERSION']}",
        :hadoop_version => "#{ENV['HADOOP_VERSION']}",
        :slave_instance_type => nil,
        :user => "ekoontz",
        :s3_bucket => "ekoontz-amis",
        :debug => false,
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
      
      arch=@@slave_arch

      if options[:label]
        image_label = options[:label]
      else
        image_label = "hbase-#{hbase_version}-#{arch}"
      end

      existing_image = find_owned_image(image_label)
      
      if existing_image
        puts "Existing image: #{existing_image.imageId} already registered for image name #{image_label}. Call HImage::deregister_image('#{existing_image.imageId}'), if desired."
        
        return existing_image.imageId
      end
      
      #FIXME: check s3 source tarballs permissions to make sure that the image creation will work before
      # going to the trouble of creating an instance to create the image.

      puts "Creating and registering image: #{image_label}"
      puts "Starting a AMI with ID: #{@@default_base_ami_image}."
      
      launch = do_launch({
                           :image_id => @@default_base_ami_image,
                           :key_name => "root",
                           :instance_type => "m1.large"
                         },"image-creator")
      
      if (launch && launch[0])
        image_creator = launch[0]
      else 
        raise "Could not launch image creator."
      end
      
      image_creator_hostname = image_creator.dnsName
      puts "Started image creator: #{image_creator_hostname}"
      
      puts "Copying scripts."
      until_ssh_able([image_creator])
      
      scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/functions.sh","/mnt")
      scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/image/create-hbase-image-remote","/mnt")
      scp_to(image_creator_hostname,"#{ENV['HOME']}/hbase-ec2/bin/image/ec2-run-user-data","/etc/init.d")
      
      # Copy private key and certificate (for bundling image)
      scp_to(image_creator_hostname,"#{ENV['HOME']}/.ec2/root.pem","/mnt")
      scp_to(image_creator_hostname,"#{ENV['HOME']}/.ec2/cert.pem","/mnt")

      if (major_version(hbase_version) == 0) and (minor_version(hbase_version) < 21)
        #Older format.
        hbase_file = "hbase-#{hbase_version}.tar.gz"
      else
        #Newer format.
        hbase_file ="hbase-#{hbase_version}-bin.tar.gz"
      end
      
      hbase_url = "http://ekoontz-tarballs.s3.amazonaws.com/#{hbase_file}"
      
      hadoop_url = "http://ekoontz-tarballs.s3.amazonaws.com/hadoop-#{hadoop_version}.tar.gz"
      lzo_url = "http://tm-files.s3.amazonaws.com/hadoop/lzo-linux-0.20-tm-2.tar.gz"
      java_url = "http://mlai.jdk.s3.amazonaws.com/jdk-6u20-linux-#{arch}.bin"
      
      puts "running /mnt/create-hbase-image-remote on image builder: #{image_creator_hostname}; hbase_version=#{hbase_version}; hadoop_version=#{hadoop_version}.."

      ssh_to(image_creator_hostname,
             "sh -c \"ARCH=#{arch} HBASE_VERSION=#{hbase_version} HADOOP_VERSION=#{hadoop_version} HBASE_FILE=#{hbase_file} HBASE_URL=#{hbase_url} HADOOP_URL=#{hadoop_url} LZO_URL=#{lzo_url} JAVA_URL=#{java_url} AWS_ACCOUNT_ID=#{@@owner_id} S3_BUCKET=#{options[:s3_bucket]} AWS_SECRET_ACCESS_KEY=#{ENV['AMAZON_SECRET_ACCESS_KEY']} AWS_ACCESS_KEY_ID=#{ENV['AMAZON_ACCESS_KEY_ID']} /mnt/create-hbase-image-remote\"",
             HCluster.image_output_handler(options[:debug]),
             HCluster.image_output_handler(options[:debug]))
      
      puts(" .. done.")

      # Register image
      image_location = "#{s3_bucket}/hbase-#{hbase_version}-#{arch}.manifest.xml"
      
      # FIXME: notify maintainers: 
      # http://amazon-ec2.rubyforge.org/AWS/EC2/Base.html#register_image-instance_method does not 
      # mention :name param (only :image_location).
      puts "registering image label: #{image_label} at manifest location: #{image_location}"
      registered_image = @@shared_base_object.register_image({
                                                               :name => image_label,
                                                               :image_location => image_location,
                                                               :description => "HBase Cluster Image: HBase Version: #{hbase_version}; Hadoop Version: #{hadoop_version}"
                                                             })
      
      puts "image registered."
      if (!(options[:debug] == true))
        puts "shutting down image-builder #{image_creator.instanceId}"
        @@shared_base_object.terminate_instances({
                                                   :instance_id => image_creator.instanceId
                                                 })
      else
        puts "not shutting down image creator: #{image_creator.dnsName}"
      end
      puts "referring to registered image: #{registered_image.to_yaml}"
      registered_image.imageId
    end
    
    def HCluster.image_output_handler(debug)
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
      launch_master
      launch_slaves
      if @launch_aux
        launch_aux
      end
      
    # if threaded, we would set to "pending" and then 
      # use join to determine when state should transition to "running".
      #    @launchTime = master.launchTime

      @state = "final initialization,,"
      #for portability, HCluster::run_test looks for /usr/local/hadoop/hadoop-test.jar.
      ssh("ln -s /usr/local/hadoop/hadoop-test-*.jar /usr/local/hadoop/hadoop-test.jar")

      @state = "running"
    end
    
    def init_hbase_cluster_secgroups
      # create security groups if necessary.
      groups = describe_security_groups
      found_master = false
      found_rs = false
      found_zk = false
      found_aux = false
      
      groups['securityGroupInfo']['item'].each { |group| 
        if group['groupName'] =~ /^#{@security_group_prefix}$/
          found_rs = true
        end
      if group['groupName'] =~ /^#{@security_group_prefix}-master$/
        found_master = true
      end
        if group['groupName'] =~ /^#{@security_group_prefix}-zk$/
          found_zk = true
        end
        if group['groupName'] =~ /^#{@security_group_prefix}-aux$/
          found_aux = true
        end
      }
      
      if (found_aux == false && options[:launch_aux] == true)
        puts "creating new security group: #{@security_group_prefix}-aux.."
        create_security_group({
                                :group_name => "#{@security_group_prefix}-aux",
                                :group_description => "Group for HBase Auxiliaries."
                              })
      end
      
      if (found_rs == false) 
        puts "creating new security group: #{@security_group_prefix}.."
        create_security_group({
                                :group_name => "#{@security_group_prefix}",
                                :group_description => "Group for HBase Slaves."
                              })
      end
      
      if (found_master == false) 
        puts "creating new security group: #{@security_group_prefix}-master.."
        create_security_group({
                                :group_name => "#{@security_group_prefix}-master",
                                :group_description => "Group for HBase Master."
                              })
        puts "..done"
      end
      
      if (found_zk == false) 
        puts "creating new security group: #{@security_group_prefix}-zk.."
        create_security_group({
                                :group_name => "#{@security_group_prefix}-zk",
                                :group_description => "Group for HBase Zookeeper quorum."
                              })
        puts "..done"
      end
      
      groups2 = ["#{@security_group_prefix}","#{@security_group_prefix}-master","#{@security_group_prefix}-zk"]
      if (options[:launch_aux] == true)
        groups2.push("#{@security_group_prefix}-aux")
      end
      
      # <allow ssh to each instance from anywhere.>
      groups2.each {|group|
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
        rescue NoMethodError
          # AWS::EC2::Base::HCluster internal error: fix AWS::EC2::Base
          puts "Sorry, AWS::EC2::Base internal error; please retry launch."
          return
        end
        
        #reciprocal full access for each security group.
        groups2.each {|other_group|
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
        }
      }
      
    end
    
    def HCluster.do_launch(options,name="",on_boot = nil)
      instances = @@shared_base_object.run_instances(options)
      watch(name,instances)
      if on_boot
        on_boot.call(instances.instancesSet.item)
      end
      return instances.instancesSet.item
    end
    
    def HCluster.watch(name,instances,begin_output = "[launch:#{name}",end_output = "]\n",debug_level = @@debug_level)
      # note: this aws_connection is separate for this watch() function call:
      # this will hopefully allow us to run watch() in a separate thread if desired.
      aws_connection = AWS::EC2::Base.new(:access_key_id=>ENV['AMAZON_ACCESS_KEY_ID'],:secret_access_key=>ENV['AMAZON_SECRET_ACCESS_KEY'])
      
      print begin_output
      STDOUT.flush
      
      wait = true
      until wait == false
        wait = false
        if instances.instancesSet == nil
          raise "instances.instancesSet is nil."
        end
        if instances.instancesSet.item == nil
          raise "instances.instancesSet.item is nil."
        end
      instances.instancesSet.item.each_index {|i| 
          instance = instances.instancesSet.item[i]
          # get status of instance instance.instanceId.
          begin
            begin
              instance_info = aws_connection.describe_instances({:instance_id => instance.instanceId}).reservationSet.item[0].instancesSet.item[0]
              status = instance_info.instanceState.name
            rescue OpenSSL::SSL::SSLError
              puts "aws_connection.describe_instance() encountered an SSL error - retrying."
              status = "waiting"
            end

            if (!(status == "running"))
              wait = true
            else
              #instance is running 
              if debug_level > 0
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
      @zks = HCluster.do_launch(options,"zk",lambda{|zks|setup_zookeepers(zks)})
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
      @master = HCluster.do_launch(options,"master",lambda{|instances| setup_master(instances[0])})[0]
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
      @slaves = HCluster.do_launch(options,"rs",lambda{|instances|setup_slaves(instances)})
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
    
    def setup_zookeepers(zks)
      #when zookeepers are ready, copy info over to them..
      #for each zookeeper, copy ~/hbase-ec2/bin/hbase-ec2-init-zookeeper-remote.sh to zookeeper, and run it.
      HCluster::until_ssh_able(zks)
      zks.each {|zk|
        if (@debug_level > 0)
          puts "zk dnsname: #{zk.dnsName}"
        end
        HCluster::scp_to(zk.dnsName,File.dirname(__FILE__) +"/../bin/hbase-ec2-init-zookeeper-remote.sh","/var/tmp")
        #note that ZOOKEEPER_QUORUM is not yet set, but we don't 
        # need it set to start the zookeeper(s) themselves, 
        # so we can remove the ZOOKEEPER_QUORUM=.. from the following.
        HCluster::ssh_to(zk.dnsName,
                         "sh -c \"ZOOKEEPER_QUORUM=\\\"#{zookeeper_quorum}\\\" sh /var/tmp/hbase-ec2-init-zookeeper-remote.sh\"",
#                         HCluster::summarize_output,HCluster::summarize_output,
                         HCluster::echo_stdout,HCluster::echo_stderr,
                         "[setup:zk:#{zk.dnsName}",
                         "]\n")
      }
    end

    def setup_master(master)
      #cluster's dnsName is same as master's.
      @dnsName = master.dnsName
      @master = master
      
      HCluster::until_ssh_able([master])
      
      @master.state = "running"
      # <ssh key>
      HCluster::scp_to(master.dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
      #FIXME: should be 400 probably.
      HCluster::ssh_to(master.dnsName,"chmod 600 /root/.ssh/id_rsa",HCluster::consume_output,HCluster::consume_output,nil,nil)
      # </ssh key>
      
      # <master init script>
      init_script = File.dirname(__FILE__) +"/../bin/#{@@remote_init_script}"
      HCluster::scp_to(master.dnsName,init_script,"/root/#{@@remote_init_script}")
      HCluster::ssh_to(master.dnsName,"chmod 700 /root/#{@@remote_init_script}",HCluster::consume_output,HCluster::consume_output,nil,nil)
      # NOTE : needs zookeeper quorum: requires zookeeper to have come up.
      HCluster::ssh_to(master.dnsName,"sh /root/#{@@remote_init_script} #{master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
                         HCluster::echo_stdout,HCluster::echo_stderr,
#                       HCluster::summarize_output,HCluster::summarize_output,
                       "[setup:master:#{master.dnsName}","]\n")
    end
    
    def setup_slaves(slaves) 
      init_script = File.dirname(__FILE__) +"/../bin/#{@@remote_init_script}"
      #FIXME: requires that both master (master.dnsName) and zookeeper (zookeeper_quorum) to have come up.
      puts "setup_slaves: calling until_ssh_able()"
      HCluster::until_ssh_able(slaves)
      puts "setup_slaves: done: slaves are sshable now."
      slaves.each {|slave|
        # <ssh key>
        HCluster::scp_to(slave.dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
        #FIXME: should be 400 probably.
        HCluster::ssh_to(slave.dnsName,"chmod 600 /root/.ssh/id_rsa",HCluster::consume_output,HCluster::consume_output,nil,nil)
        # </ssh key>
        
        HCluster::scp_to(slave.dnsName,init_script,"/root/#{@@remote_init_script}")
        HCluster::ssh_to(slave.dnsName,"chmod 700 /root/#{@@remote_init_script}",HCluster::consume_output,HCluster::consume_output,nil,nil)
        HCluster::ssh_to(slave.dnsName,"sh /root/#{@@remote_init_script} #{@master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
                         HCluster::echo_stdout,HCluster::echo_stderr,
#                         HCluster::summarize_output,HCluster::summarize_output,
                         "[setup:rs:#{slave.dnsName}","]\n")
      }
    end
    
    def setup_aux(aux) 
      #NOTE:if setup process is multithreaded, setup_aux requires 
      # master.dnsName and zookeeper_quorum to be known.
      until_ssh_able([aux])
      dnsName = aux.dnsName
      
      # <ssh key>
      HCluster::scp_to(dnsName,"#{EC2_ROOT_SSH_KEY}","/root/.ssh/id_rsa")
      #FIXME: should be 400 probably.
      HCluster::ssh_to(dnsName,"chmod 600 /root/.ssh/id_rsa",HCluster::consume_output,HCluster::consume_output,nil,nil)
      # </ssh key>
      
      init_script = "#{ENV['HOME']}/hbase-ec2/bin/#{@@remote_init_script}"
      HCluster::scp_to(dnsName,init_script,"/root/#{@@remote_init_script}")
      HCluster::ssh_to(dnsName,"chmod 700 /root/#{@@remote_init_script}",HCluster::consume_output,HCluster::consume_output,nil,nil)
      HCluster::ssh_to(dnsName,"sh /root/#{@@remote_init_script} #{@master.dnsName} \"#{zookeeper_quorum}\" #{@num_regionservers}",
                       HCluster::summarize_output,HCluster::summarize_output,"[setup:aux:#{dnsName}","]\n")
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
    
    def describe_instances(options = {})
      retval = nil
      @lock.synchronize {
        retval = super(options)
      }
      retval
    end
    
    #overrides parent: tries to find image using owner_id, which will be faster to iterate through (in .detect loop)
    # if not found, tries all images.
    def HCluster.describe_images(options,image_label = nil,search_all_visible_images = true)
      if image_label
        options = {
          :owner_id => @@owner_id
        }.merge(options)
        
        retval = @@shared_base_object.describe_images(options)
        #filter by image_label
        retval2 = retval['imagesSet']['item'].detect{
          |image| image['name'] == image_label
        }
        
        if (retval2 == nil and search_all_visible_images == true)
          options.delete(:owner_id)
          puts "image '#{image_label}' not found in owner #{@@owner_id}'s images; looking in all images (may take a while..)"
          retval = @@shared_base_object.describe_images(options)
          #filter by image_label
          retval2 = retval['imagesSet']['item'].detect{
            |image| image['name'] == image_label
          }
        end
        retval2
      else
        @@shared_base_object.describe_images(options)
      end
    end
    
    def zk_image
      get_image(@zk_image_label)
    end
    
    def regionserver_image
      get_image(@slave_image_label)
    end
    
    def master_image
      get_image(@master_image_label)
    end
    
    def HCluster.find_owned_image(image_label)
      return describe_images({:owner_id => @@owner_id},image_label,false)
    end
    
    def get_image(image_label)
      matching_image = HCluster.describe_images({:owner_id => @@owner_id},image_label)
      if matching_image
        matching_image
      else
        raise HClusterStartError,
        "describe_images({:owner_id => '#{@@owner_id}'},'#{image_label}'): couldn't find #{image_label}, even in all of Amazon's viewable images."
      end
    end
    
    def if_null_image(retval,image_label)
      if !retval
        raise HClusterStartError, 
        "Could not find image '#{image_label}' in instances viewable by AWS Account ID: '#{@@owner_id}'."
      end
    end
    
    def run_test(test,stdout_line_reader = lambda{|line,channel| puts line},stderr_line_reader = lambda{|line,channel| puts "(stderr): #{line}"})
      #fixme : fix hardwired version (first) then path to hadoop (later)
      ssh("/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/hadoop-test.jar #{test}",
          stdout_line_reader,
          stderr_line_reader)
    end
    
    #If command == nil, open interactive channel.
    def HCluster.ssh_to(host,command = nil,
                        stdout_line_reader = lambda{|line,channel| puts line},
                        stderr_line_reader = lambda{|line,channel| puts "(stderr): #{line}"},
                        begin_output = nil,
                        end_output = nil)
      # variant of ssh with different param ordering.
      ssh_with_host(command,stdout_line_reader,stderr_line_reader,host,begin_output,end_output)
    end
    
    def HCluster.ssh_with_host(command,stdout_line_reader,stderr_line_reader,host,begin_output,end_output)
      if command == nil
        interactive = true
      end
      
      if false
        until command == "exit\n"
          print "#{host}>"
          command = gets
        end
        return
      end
      
      if begin_output
        print begin_output
        STDOUT.flush
      end
      # http://net-ssh.rubyforge.org/ssh/v2/api/classes/Net/SSH.html#M000013
      # paranoid=>false because we should ignore known_hosts, since AWS IPs get frequently recycled
      # and their servers' private keys will vary.
      
      until command == "exit\n"
        if interactive == true
          print "#{host} $ "
          command = gets
        end
        Net::SSH.start(host,'root',
                       :keys => ["~/.ec2/root.pem"],
                       :paranoid => false
                       ) do |ssh|
          stdout = ""
          channel = ssh.open_channel do |ch|
            channel.exec(command) do |ch, success|
              #FIXME: throw exception(?)
              puts "channel.exec('#{command}') was not successful." unless success
            end
            channel.on_data do |ch, data|
              stdout_line_reader.call(data,channel)
              # example of how to talk back to server.
              #          channel.send_data "something for stdin\n"
            end
            channel.on_extended_data do |channel, type, data|
              stderr_line_reader.call(data,channel)
            end
            channel.wait
            if !(interactive == true)
              #Cause exit from until(..) loop.
              command = "exit\n"
            end
            channel.on_close do |channel|
              # cleanup, if any..
            end
          end
        end
      end
      if end_output
        print end_output
        STDOUT.flush
      end
    end
    
    # Send a command and handle stdout and stderr 
    # with supplied anonymous functions (puts by default)
    # to a specific host (master by default).
    # If command == nil, open interactive channel.
    def ssh(command = nil,
            stdout_line_reader = HCluster.echo_stdout,
            stderr_line_reader = HCluster.echo_stderr,
            host = self.master.dnsName,
            begin_output = nil,
            end_output = nil)
      if (host == @dnsName)
        raise HClusterStateError,
        "This HCluster has no master hostname. Cluster summary:\n#{self.to_s}\n" if (host == nil)
      end
      
      HCluster.ssh_with_host(command,stdout_line_reader,stderr_line_reader,host,begin_output,end_output)
    end
    
    def HCluster.scp_to(host,local_path,remote_path)
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
      if (@state)
        retval = "HCluster (state='#{@state}'): #{@num_regionservers} regionserver#{((@numregionservers == 1) && '') || 's'}; #{@num_zookeepers} zookeeper#{((@num_zookeepers == 1) && '') || 's'}; hbase_version:#{options[:hbase_version]};"
        if (@aux)
          retval = retval + "; 1 aux"
        end
        retval = retval + "."
      end
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
    
    def HCluster.until_ssh_able(instances,debug_level = @@debug_level)
      # do not return until every instance in the instances array is ssh-able.
      debug_level = 1
      instances.each {|instance|
        connected = false
        until connected == true
          begin
            puts "#{instance.dnsName} trying to ssh.."
            ssh_to(instance.dnsName,"true",HCluster::consume_output,HCluster::consume_output,nil,nil)
            puts "#{instance.dnsName} is sshable."
            connected = true
          rescue Net::SSH::AuthenticationFailed
            if debug_level > 0
              puts "host: #{instance.dnsName} not ready yet - waiting.."
            end
            sleep 5
          rescue Errno::ECONNREFUSED
            if debug_level > 0
              puts "host: #{instance.dnsName} not ready yet (connection refused) - waiting.."
            end
            sleep 5
          rescue Errno::ECONNRESET
            if debug_level > 0
              puts "host: #{instance.dnsName} not ready yet (connection reset) - waiting.."
            end
            sleep 5
          rescue Errno::ETIMEDOUT
            if debug_level > 0
              puts "host: #{instance.dnsName} not ready yet (timed out) - waiting.."
            end
            sleep 5
          rescue OpenSSL::SSL::SSLError
            if debug_level > 0
              puts "host: #{instance.dnsName} not ready yet (ssl error) - waiting.."
            end
            sleep 5
          end
        end
      }
    end
    
    def HCluster.echo_stdout
      return lambda{|line,channel|
        puts line
      }
    end
    
    def HCluster.echo_stderr 
      return lambda{|line,channel|
        puts "(stderr): #{line}"
      }
    end
    
    def HCluster.consume_output 
      #don't print anything for each line.
      return lambda{|line|
      }
    end
    
    def HCluster.summarize_output
      #output one '.' per line.
      return lambda{|line,channel|
        putc "."
      }
    end
    
    def HCluster.major_version(version_string)
      begin
        /(hbase-)?([0-9+])/.match(version_string)[2].to_i
      rescue NoMethodError
        "no minor version found for version #{version_string}."
      end
    end
    
    def HCluster.minor_version(version_string)
      begin
        /(hbase-)?[0-9+].([0-9]+)/.match(version_string)[2].to_i
      rescue NoMethodError
        "no minor version found for version '#{version_string}'."
      end
    end

    def HCluster.label_to_hbase_version(label)
      begin
        /hbase-([0-9+]\.[0-9]+\.[0-9]+)/.match(label)[1]
      rescue NoMethodError
        "could not convert label: #{label} to an hbase version."
      end
    end
  end

end
