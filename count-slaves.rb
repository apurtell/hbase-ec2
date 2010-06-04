include Java
begin
  admin = HBaseAdmin.new(HBaseConfiguration.new)
  status = admin.getClusterStatus()
  print "%d" % [ status.getServers() ]
rescue
  print "0"
end
exit
