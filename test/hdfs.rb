#!/usr/bin/ruby

load("~/hbase-ec2/lib/hcluster.rb");
c = AWS::EC2::Base::HCluster
c['hdfs2'].status
c['hdfs2'].launch
c['hdfs2'].run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")



