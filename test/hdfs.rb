#!/usr/bin/ruby

load("~/hbase-ec2/lib/hcluster.rb");
hdfs = AWS::EC2::Base::HCluster.new("hdfs");
hdfs.status
hdfs.launch
hdfs.run_test("TestDFSIO -write -nrFiles 10 -fileSize 1000")
hdfs.terminate



