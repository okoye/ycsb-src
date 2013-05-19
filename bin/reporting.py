'''
automatically parse throughput info for average latency
'''
import optparse

def parse(line):
  time,rest = line.split("sec:")
  time = int(time.strip())
  try:
    avg_latency = rest.split("READ AverageLatency(us)=")[1].replace("]", "")
  except IndexError: #no ops
    avg_latency = -1
  return (time, avg_latency)

def dummy_emitter(start, end):
  for i in xrange(start, end):
    print "%s\t%s"%(i, -1)

def main(file_name, delta_time):
  delta_time = 10
  count = 0 - delta_time
  for line in open(file_name):
    count += delta_time
    if 'failed' in line:
      continue
    if 'current ops/sec' in line:
      time, avg_latency = parse(line)
      if time != count:
        dummy_emitter(count, time)
        count = time
      print '%s\t%s'%(time, avg_latency)

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option('-f', '--file', help='file name',
                    dest='f')
  parser.add_option('-d', '--delta', help='time step',
                    dest='d', default=10)
  (opts, args) = parser.parse_args()
  main(opts.f, int(opts.d))
      
