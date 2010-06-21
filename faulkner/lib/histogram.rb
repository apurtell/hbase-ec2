# This code is distributed freely
# K.Kodama 2005-03-01

class Histogram
  attr :min;
  attr :max;
  attr :w; # class width
  attr :f; # array of frequency

  attr :num;
  attr :s1;
  attr :s2;
  attr :s3;
  attr :s4;

  def h_class(x)
    ((x-@min)/@w).truncate
  end;
  def bar(x,unit=1)
    n=(x.to_f/unit).ceil; ("----+"*(n/5+1))[0,n]
  end;

  def initialize(min,max,width)
    @min=min; @max=max; @w=width; @f=Array::new(h_class(@max)+1,0)
    @num=0; @s1=0.0; @s2=0.0; @s3=0.0; @s4=0.0;
  end;
  def push(x)
    @num+=1; @s1+=x; @s2+=(x**2); @s3+=(x**3); @s4+=(x**4); 
    # if x<@min or @max<x then return; end;
    @f[h_class([[@min,x].max, @max].min)]+=1
  end;
  def clear
    initialize(@min,@max,@w)
  end
  # statistics
  def size
    @num # s=0; @f.each{|n| s+=n}; return s
  end;
  def sum
    @s1
  end;
  def avg
    @s1/@num # average or arithmetic mean
  end
  def variance
    (@s2/@num)-(@s1/@num)**2 # v=E((x-m)^2)=E(x^2)-m^2  where m=E(X).
  end;
  def sd
    Math::sqrt(variance) # standard deviation.
  end;
  def cv
    sd/avg # coefficient of variation
  end;
  def skewness
    # skewness E((x-m)^3)/s^3=(E(x^3)-3*m*E(x^2) + 2 m ^3)/s^3
    m=avg; s=sd; ((@s3-3*m*@s2)/@num+2*m*m*m)/(s*s*s)
  end;
  def kurtosis
    # kurtosis E((x-m)^4)/s^4=(E(x^4)-4mE(x^3)+6m^2E(x^2)-3m^4)/s^4
    m=avg; v=variance; ((@s4-4*m*@s3+6*m*m*@s2)/@num-3*m**4)/(v*v)
  end;
  def to_s(unit=1,indent=0)
    form = sprintf("%%%dd-.:%%s %%d\n", @max.to_s.size)
    s=""
    for i in 0..h_class(@max) do
      s += (0...indent).map{' '}.join
      s += sprintf(form,@min+i*@w,bar(@f[i],unit) ,@f[i])
    end
    return s
  end;
  def report(unit=1,indent=0)
    printf("%s", to_s(unit,indent))
    printf((0...indent).map{' '}.join)
    printf("number: %d, average: %f, variance: %f \n",size, avg, variance)
    if @num > 0 then
      printf((0...indent).map{' '}.join)
      printf("standard deviation: %f, coefficient of variation: %f\n",sd, cv)
    end
    # printf("skewness: %f, kurtosis: %f\n", skewness, kurtosis)
  end;
end;

if $0 == __FILE__ then
  # Example.
  hist=Histogram.new(0,100,10) # lower bound/upper bound/class width
  hist.push(51) # push data
  hist.push(62)
  hist.push(31)
  hist.push(200) # This value is out of bound 0-99.
  hist.push(-200) # This value is out of bound 0-99.
  hist.report # print histogram
end;
