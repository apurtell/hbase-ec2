require 'hcluster'

class AWS::EC2::Base::HCluster::TestDFSIO < AWS::EC2::Base::HCluster
  def initialize(name = "hdfs", options = {} )
    super(name,options)
  end

  def test(nrFiles=10,fileSize=1000)
    state = "begin"
    stderr = ""
    stdout = ""
    retval_hash = {}
    result_pairs = {}
    av_lines = []
    run_test("TestDFSIO -write -nrFiles #{nrFiles} -fileSize #{fileSize}",
             lambda{|line|
               stdout = stdout + line
               puts line
             },
             lambda{|line|
               stderr = stderr + line
               #implement finite state machine
               if line =~ /-+ TestDFSIO -+/
                 state = "results"
               end

               if state == "results"
                 av_lines.push(line)
               else
                 putc "."
               end
             })

    av_section = av_lines.join("\n")

    av_section.split(/\n/).each {|av_line|
      av_pair = av_line.split(/: /)
      if (av_pair[2])
        result_pairs[trim(av_pair[1])] = trim(av_pair[2])
      end
    }

    puts

    retval_hash['pairs'] = result_pairs
    retval_hash['stdout'] = stdout
    retval_hash['stderr'] = stderr

    retval_hash

  end
end
