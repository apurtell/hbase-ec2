# Some of the below cribbed from dj_ryan's o.a.h.h.io.hfile.RandomDistribution

class Distribution
  def initialize(min, max)
    @min = min
    @max = max
  end
  def min
    return @min
  end
  def max
    return @max
  end
  def binsearch(l, k)
    pos = 0
    lo = 0
    hi = l.length - 1
    begin
      pos = (lo + hi) / 2
      m = l[pos]
      if k < m then
        hi = pos - 1
      elsif k > m then
        pos = pos + 1
        lo = pos
      else
        return pos
      end
    end while lo <= hi
    return -pos - 1  # insertion point, like Collections.binarySearch
  end
end

class FlatDistribution < Distribution
  # P(i)=1/(max-min)
  def rand
    return Kernel::rand(@max - @min) + @min
  end
end

class ZipfDistribution < Distribution
  # P(i)/P(j)=((j-min+1)/(i-min+1))^sigma
  def initialize(min, max, sigma = 1.2, epsilon = 0.001)
    @k = []
    @v = []
    @min = min
    @max = max
    sum = 0.0
    last = -1
    min.upto(max - 1) do |i|
      sum = sum + Math.exp(-sigma * Math.log(i - min + 1))
      if (last == -1) || (i * (1 - epsilon) > last) then
        @k.push(i)
        @v.push(sum)
        last = i
      end
    end
    if last != max - 1 then
      @k.push(max - 1)
      @v.push(sum)
    end
    @v[@v.length - 1] = 1.0
    (@v.length - 2).downto(0) { |i| @v[i] = @v[i] / sum }
  end
  def rand
    i = binsearch(@v, Kernel::rand)
    if i > 0 then
      i = i + 1
    else
      i = -(i + 1)
    end
    if i > @v.length then
      i = @v.length - 1
    end
    if i == 0 then
      return @k[0]
    else
      return @k[i] - Kernel::rand(@k[i] - @k[i-1])
    end
  end
end

class Zipf2Distribution < Distribution
  # P(i)/P(j)=((j-min+1)/(i-min+1))^sigma
  def initialize(min, max, sigma = 1.2, epsilon = 0.001)
    @k = []
    @v = []
    @min = min
    @max = max
    sum = 0.0
    last = -1
    min.upto(max - 1) do |i|
      sum = sum + Math.exp(-sigma * Math.log(i - min + 1))
      if (last == -1) || (i * (1 - epsilon) > last) then
        @k.push(i)
        @v.push(sum)
        last = i
      end
    end
    if last != max - 1 then
      @k.push(max - 1)
      @v.push(sum)
    end
    @v[@v.length - 1] = 1.0
    (@v.length - 2).downto(0) { |i| @v[i] = @v[i] / sum }
  end
  def _rand
    i = binsearch(@v, Kernel::rand)
    if i > 0 then
      i = i + 1
    else
      i = -(i + 1)
    end
    if i > @v.length then
      i = @v.length - 1
    end
    if i == 0 then
      return @k[0]
    else
      return @k[i] - Kernel::rand(@k[i] - @k[i-1])
    end
  end
  def rand
    return 2 ** (Math.log(_rand)/Math.log(2))
  end
end

class BinomialDistribution < Distribution
  # P(k)=select(n, k)*p^k*(1-p)^(n-k) (k = 0, 1, ..., n)
  # P(k)=select(max-min-1, k-min)*p^(k-min)*(1-p)^(k-min)*(1-p)^(max-k-1)
  def select(n, k)
    d = 1.0
    (k + 1).upto(n) { |i| d *= i.to_f / (i - k) }
    return d
  end
  def power(p, k)
    return Math.exp(k * Math.log(p))
  end
  def initialize(min, max, p)
    @min = min
    @max = max
    n = max - min - 1
    if n > 0 then
      @v = []
      sum = 0.0
      0.upto(n) do |i|
        sum += select(@n, i) * power(p, i) * power(1 - p, n - i)
        @v[i] = sum
      end
      0.upto(n) { |i| @v[i] /= sum }
    end
  end
  def rand
    if !defined?(@v) then
      return @min
    end
    i = binsearch(@v, Kernel::rand)
    if i > 0 then
      i = i + 1
    else
      i = -(i + 1)
    end
    if i >= @v.length then
      i = @v.length - 1
    end
    return i + @min
  end
end
