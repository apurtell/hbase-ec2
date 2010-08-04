require 'hcluster.rb'

module Hadoop

class Faulkner < HCluster

  def launch(options = {})
    options = {
      :setup_kerberized_hbase => true,
      :hbase_debug_level => true
    }.merge(options)
    super.launch(options)
  end

  def test()
    ssh("mkdir -p faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/faulkner.rb","faulkner")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/distributions.rb","faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/histogram.rb","faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/uuid.rb","faulkner/lib")
    ssh("/usr/local/hbase/bin/hbase shell /root/faulkner/faulkner.rb")
  end
end

end
