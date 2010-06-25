#
# Faulkner
# apurtell@apache.org
#
# Faulkner is a tool for stuffing HBase full of data.
#
# MARY FAULKNER (1903-1973), South African romance novelest, wrote 904 books
# and was ranked by the Guinness Book of World Records as history's most
# prolific novelist.
#
# Usage:
#
#   hbase shell /path/to/faulkner.rb [options]
#
#   where [options] are:
#
#     --table=<name>            Table name (default 'TestTable')
#     --rows=<rows>             Number of rows to write (default 1T)
#     --min=<min>               Minimum value size, in bytes (default 4)
#     --max=<max>               Maximum value size, in bytes (default 1 MB)
#     --threads=<threads>       Number of concurrent writers (default 1)
#     --keygen=<keygen>         Key generator
#       <keygen> can be one of:
#         bignum                  0-padded BigNum (default)
#         uuid                    Random UUIDs
#         ts                      Salted timestamp
#     --dist=<dist>             Probability distribution of value size
#       <dist> can be one of:
#         flat                    Flat distribution (default)
#         zipf                    Zipf power law distribution
#         zipf2                   Zipf distribution, powers of 2 only
#     --debug=true|false        Set HBase and ZK logging to DEBUG (def. false)
#     --retries=<retries>       Maximum number of retries (default 10)
#     --writeToWAL=true|false   Toggle writeToWAL on puts (default true)
#     --useLZO=true|false       Set COMPRESSION to 'LZO' in table schema
#                                 (default is false)
#     --maxRegions=<limit>      Terminate after <limit> regions created
#

import java.util.Arrays

dir = File.expand_path(File.dirname(__FILE__))
%w[
  distributions
  histogram
  uuid
].each do |f|
  eval(IO.read("%s/lib/%s.rb" % [dir,f]), binding)
end

# ruby will implicitly use bignums if value becomes too big to fit into a
# machine integer
class BigNumKeyGenerator
  def initialize(min, max)
    @min = min
    @max = max
    @num = min
    @fmt = "%0" + Math.log10(max).to_i.to_s + "d"
  end
  def next
    @num = (@num + 1) % @max
    return format(@fmt, @num).to_java_bytes
  end
end

# salted timestamp
class SaltedTSGenerator
  def initialize(count)
    @count = count
    @fmt = "%0" + Math.log10(count).to_i.to_s + "d%d"
  end
  def next
    now = Time.now
    return format(@fmt, rand(@count), (now.tv_sec*1000*1000) + now.tv_usec).to_java_bytes
  end
end

# random UUIDs
class UUIDKeyGenerator
  def next
    uuid = UUID.create_random
    return uuid.to_s.to_java_bytes
  end
end

class Writer
  PERIOD = 10000
  def initialize(conf, name, keygen, dist, rows, writeToWAL)
    @info = 'info'.to_java_bytes
    @table = HTable.new(conf, name)
    # like PE
    @table.setAutoFlush(false)
    @table.setWriteBufferSize(1024*1024*12)
    @keygen = keygen
    @dist = dist
    @rows = rows
    @writeToWAL = writeToWAL
    if @dist.max != @dist.min then
      @size_hist = Histogram.new(dist.min, dist.max - 1, (dist.max - dist.min) / 10)
    end
  end
  def write_one_row
    before = Time.now
    p = Put.new(@keygen.next)
    # make sure the column is always unique in case we're storing into an
    # existing row
    col = format("value-%d", ((before.tv_sec * 1000 * 1000) + before.tv_usec)).to_java_bytes
    if @dist.max != @dist.min then
      l = @dist.rand
      @size_hist.push(l)
      p.add(@info, col, Arrays.copyOf(VALUE, l))
    else
      p.add(@info, col, VALUE)
    end
    p.setWriteToWAL(@writeToWAL)
    retries = MAX_RETRIES
    begin
      @table.put(p)
      break
    rescue Exception => e
      if retries < 1 then
        raise e
      end
      retries--
      sleep(1)
    end while true
    after = Time.now
    return (((after.tv_sec * 1000 * 1000) + after.tv_usec) - ((before.tv_sec * 1000 * 1000) + before.tv_usec))
  end
  def run
    puts "writer %s started: 0/%d\n" % [ Thread.current, @rows ]
    period_ms = 0
    1.upto(@rows) do |i|
      ms = write_one_row / 1000
      period_ms += ms
      if (i % PERIOD) == 0 then
        puts "writer %s\n" % Thread.current
        regions = @table.getStartKeys.length
        puts "  %d/%d rows @ %d regions\n" % [ i, @rows, regions ]
        puts "    %f ms/row\n" % [ period_ms.to_f / i, PERIOD ]
        if @dist.max != @dist.min then
          puts "  sizes:"
          puts @size_hist.to_s(@size_hist.size / 40, 4)
          puts "    average: %f\n" % [ @size_hist.avg ]
        end
        period_ms = 0
        @size_hist.clear
        if defined?(MAX_REGIONS) && regions >= MAX_REGIONS then
          Process.exit
        end
      end
    end
    retries = MAX_RETRIES
    begin
      @table.flushCommits
      break
    rescue Exception => e
      if retries < 1 then
        raise e
      end
      retries--
      sleep(1)
    end while true
    puts "writer %s done\n" % Thread.current
  end
end

ARGV.each do |a|
  m = a.match('--([^=]+)=(.+)')
  if !m.nil? then
    case m[1]
    when 'min'
      MIN = m[2].to_i
    when 'max'
      MAX = m[2].to_i
    when 'rows'
      R = m[2].to_i
    when 'writers', 'threads'
      T = m[2].to_i
    when 'table'
      TABLE = m[2]
    when 'keygen'
      G = m[2].downcase
    when 'retries'
      MAX_RETRIES = m[2].to_i
    when 'maxRegions'
      MAX_REGIONS = m[2].to_i
    when 'debug'
      DEBUG = (m[2] == "true")
    when 'useLZO'
      USE_LZO = (m[2] == "true")
    when 'writeToWAL'
      WRITE_TO_WAL = (m[2] == "true")
    end
  end
end

if !defined?(TABLE) then
  TABLE = 'TestTable'
end

if !defined?(MIN) then
  MIN = 4
end

if !defined?(MAX) then
  MAX = 1024 * 1024
end

VALUE = (0...MAX).map{32.+(rand(127-32)).chr}.join.to_java_bytes

if !defined?(T) then
  T = 1
end

if !defined?(R) then
  R = 1000000000000 # 1 trillion
end

if !defined?(G) then
  G = 'uuid'
end

if !defined?(MAX_RETRIES) then
  MAX_RETRIES = 10
end

if !defined?(DEBUG) then
  DEBUG = false
end

if !defined?(USE_LZO) then
  USE_LZO = false
end

if !defined?(WRITE_TO_WAL) then
  WRITE_TO_WAL = true
end

ARGV.each do |a|
  m = a.match('--([^=]+)=(.+)')
  if !m.nil? then
    case m[1]
    when 'dist', 'distribution'
      case m[2].downcase
      when 'flat'
        D = FlatDistribution.new(MIN, MAX)
        puts "using flat distribution, min=%d max=%d\n" % [MIN,MAX]
      when 'zipf'
        D = ZipfDistribution.new(MIN, MAX)
        puts "using zipf (pareto) distribution, min=%d max=%d\n" % [MIN,MAX]
      when 'zipf2'
        D = Zipf2Distribution.new(MIN, MAX)
        puts "using zipf2 (pareto, powers of 2 only) distribution, min=%d max=%d\n" % [MIN,MAX]
      end
    end
  end
end

if !defined?(D) then
  D = ZipfDistribution.new(MIN, MAX)
  puts "using ziph (pareto) distribution, min=%d max=%d\n" % [MIN,MAX]
end

if DEBUG then
  org.apache.log4j.Logger.getLogger("org.apache.zookeeper").
    setLevel(org.apache.log4j.Level::INFO)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").
    setLevel(org.apache.log4j.Level::DEBUG)
else
  org.apache.log4j.Logger.getLogger("org.apache.zookeeper").
    setLevel(org.apache.log4j.Level::ERROR)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").
    setLevel(org.apache.log4j.Level::ERROR)
end

conf = HBaseConfiguration.new

admin = HBaseAdmin.new(conf)
if !admin.tableExists(TABLE) then
  htd = HTableDescriptor.new(TABLE)
  hcd = HColumnDescriptor.new("info".to_java_bytes)
  if USE_LZO then
    hcd.setValue('COMPRESSION','LZO')
  end
  htd.addFamily(hcd)
  admin.createTable(htd)
end

puts "using %d threads\n" % T
puts "writing %d total rows\n" % R

threads = []
1.upto(T) do |i|
  case G
  when 'uuid'
    keygen = UUIDKeyGenerator.new
  when 'ts'
    clusterStatus = admin.getClusterStatus()
    count = clusterStatus.getServers()
    puts "cluster has %d regionservers\n" % count
    keygen = SaltedTSGenerator.new(count)
  else
    keygen = BigNumKeyGenerator.new((R/T)*i, (R/T)*(i+1))
  end
  t = Thread.new(keygen) do |keygen| 
    Writer.new(conf, TABLE, keygen, D, R / T, WRITE_TO_WAL).run
  end
  threads.push(t)
end
threads.each { |t| t.join }

exit 0
