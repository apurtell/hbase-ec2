HBASE_OPTS="-Xmx2000m" hbase shell ./faulkner.rb --keygen=uuid --threads=20 --min=256 --max=1048576 --maxRegions=5000 --debug=true > ./logs/faulkner.log 2>&1 & tail -f ./logs/faulkner.log 
