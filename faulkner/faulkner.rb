#
# Faulkner is a tool for stuffing HBase full of data.
#
# Unlike PerformanceEvaluation, it uses random UUIDs as row keys to support
# writing billions of unique rows. It also generates a range of value sizes
# using probability distribution functions to produce a more realistic load.
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
#     --table=<name>           Table name (default 'TestTable')
#     --rows=<rows>            Number of rows to write (default 1B)
#     --min=<min>              Minimum value size (default 4)
#     --max=<max>              Maximum value size (default 1MB)
#     --threads=<threads>      Number of concurrent writers (default 1)
#     --dist=<dist>            Probability distribution of value size
#
#       <dist> can be one of:
#         flat                 Flat distribution
#         zipf                 Zipf (Pareto) power law distribution (default)
#         binomial             Binomial distribution
#
#     --writeToWAL=true|false  Toggle writeToWAL on puts (default false)
#     --useLZO=true|false      Set COMPRESSION to 'LZO' in table schema
#                                (default is false)
#

dir = File.expand_path(File.dirname(__FILE__))
eval(IO.read("%s/lib/distributions.rb" % dir), binding)
eval(IO.read("%s/lib/uuid.rb" % dir), binding)

class Writer
  def initialize(conf, name, dist, rows, writeToWAL)
    @info = 'info'.to_java_bytes
    @value = 'value'.to_java_bytes
    @table = HTable.new(conf, name)
    @table.setAutoFlush(false)
    @table.setWriteBufferSize(1024*1024*20)
    @dist = dist
    @rows = rows
    @writeToWAL = writeToWAL
    @str = (0...@dist.max).map{32.+(rand(127-32)).chr}.join
  end
  def write_one_row
    uuid = UUID.create_random
    p = Put.new(uuid.to_s.to_java_bytes)
    p.add(@info, @value, @str[0, @dist.rand].to_java_bytes)
    p.setWriteToWAL(@writeToWAL)
    @table.put(p)
  end
  def run
    puts "writer %s started: 0/%d\n" % [ Thread.current, @rows ]
    1.upto(@rows) do |i|
      write_one_row
      if (i % 10000) == 0 then
        puts "writer %s: %d/%d\n" % [ Thread.current, i, @rows ]
      end
    end
    @table.flushCommits
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

if !defined?(T) then
  T = 1
end

if !defined?(R) then
  R = 1000000000 # 1 billion
end

ARGV.each do |a|
  m = a.match('--([^=]+)=(.+)')
  if !m.nil? then
    case m[1]
    when 'useLZO'
      USE_LZO = (m[2] == "true")
    when 'writeToWAL'
      WRITE_TO_WAL = (m[2] == "true")
    when 'dist', 'distribution'
      case m[2].downcase
      when 'flat'
        D = FlatDistribution.new(MIN, MAX)
        puts "using flat distribution, min=%d max=%d\n" % [MIN,MAX]
      when 'ziph'
        D = ZiphDistribution.new(MIN, MAX)
        puts "using ziph (pareto) distribution, min=%d max=%d\n" % [MIN,MAX]
      when 'binomial'
        D = BinomialDistribution.new(MIN, MAX)
        puts "using binomial distribution, min=%d max=%d" % [MIN,MAX]
      end
    end
  end
end

if !defined?(D) then
  D = ZipfDistribution.new(MIN, MAX)
  puts "using ziph (pareto) distribution, min=%d max=%d\n" % [MIN,MAX]
end

if !defined?(USE_LZO) then
  USE_LZO = false
end

if !defined?(WRITE_TO_WAL) then
  WRITE_TO_WAL = false
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
1.upto(T) do
  t = Thread.new { Writer.new(conf, TABLE, D, R / T, WRITE_TO_WAL).run }
  threads.push(t)
end
threads.each { |t| t.join }

exit 0
