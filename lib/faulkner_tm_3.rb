require 'hcluster.rb'

module Hadoop

class Faulkner < HCluster

  def launch(options = {})
    super({
            :kerberized => true,
          }.merge(options))
  end

  def test()
    ssh("mkdir -p faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/webtable.sh","faulkner")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/faulkner.rb","faulkner")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/distributions.rb","faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/histogram.rb","faulkner/lib")
    scp("#{ENV['HOME']}/hbase-ec2/faulkner/lib/uuid.rb","faulkner/lib")
    ssh("mkdir -p /root/logs")
    ssh("sh /root/faulkner/webtable.sh")
  end
end

end
