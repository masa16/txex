$db = {}
$tx = []
$n_abort = 0

class Step

  def initialize(type,data_id)
    @type = type
    @data_id = data_id
  end
  attr_reader :type,:data_id

  def inspect
    '%s%d'%[@type,@data_id]
  end
end


class DataItem

  def initialize(id)
    @id = id
    @max_version = 0
    @values = {0=>0}
    @max_tid = {}
    @lock = Mutex.new
  end

  attr_reader :id, :values, :max_tid

  def read(tid)
    if tid >= @max_version
      version = @max_version
      value = @values[version]
      if tid > version + 1
        # record read history for GC
        # ri[xj] -- r[i=tid](x[j=version])
        @lock.synchronize do
          h = @max_tid[version]
          if h.nil? || tid > h
            @max_tid[version] = tid
          end
        end
      end
    else
      keys = @lock.synchronize do
        @values.keys
      end
      # find max version <= tid
      version = 0
      keys.each do |v|
        if version < v && v <= tid
          version = v
        end
      end
      value = @values[version]
      if value.nil?
        keys = @lock.synchronize do
          @values.keys
        end
        raise "id=#{@id},tid=#{tid},keys=#{keys},version=#{version},value=#{value}"
      end
    end
    return value
  end

  def write(version,value)
    @max_tid.dup.each do |v,tid|
      if v < version && version < tid
        return false # abort
      end
    end
    @lock.synchronize do
      @values[version] = value
    end
    if version > @max_version
      @max_version = version
    end
    return true
  end

  def gc(tid)
    @lock.synchronize do
      if tid.nil?
        raise "gc: id=#{@id} tid=#{tid} @values=#{@values}"
      end
      @values.keys.each do |v|
        if v > 0 && v < tid && v < @max_version
          #puts "gc: id=#{@id} tid=#{tid} ver=#{v} @values=#{@values}"
          @values.delete(v)
          @max_tid.delete(v)
        end
      end
    end
  end
end


class InvokeTID
  def initialize
    @counter = 0
    @lock = Mutex.new
    @tid_set = []
  end

  def invoke_tid
    @lock.synchronize do
      tid = (@counter += 1)
      @tid_set.push(tid)
      tid
    end
  end

  def end_tid(tid)
    tid0 = nil
    tid1 = nil
    @lock.synchronize do
      if @tid_set[0] == tid
        #puts "@tid_set=#{@tid_set}"
        tid0 = @tid_set.shift
        tid1 = @tid_set[0]
      else
        @tid_set.delete(tid)
        return
      end
    end
    if tid1 && tid1-tid0 > 100
      puts "GC: tid0=#{tid0} tid1=#{tid1} @tid_set=#{@tid_set}"
      $db.each do |id,data|
        data.gc(tid1)
      end
    end
  end
end


class Transaction

  LOCK = Mutex.new

  class Abort < Exception
  end

  def initialize(seq,invoker,thread_id)
    @seq = seq
    @invoker = invoker
    @thread_id = thread_id
  end

  def run
    begin
      @ds = {}
      tid = @invoker.invoke_tid
      @seq.each do |step|
        #p step
        case step.type
        when :r
          value = $db[step.data_id].read(tid)
          value += 1 # dummy job
          @ds[step.data_id] = value
        when :w
          unless $db[step.data_id].write(tid,@ds[step.data_id]||0)
            raise Abort
          end
        else
          raise "invalid type"
        end
      end
      #sleep 0.0001
      @invoker.end_tid(tid)
    rescue Abort
      LOCK.synchronize do
        $n_abort += 1
      end
      #sleep 0.0001
      @invoker.end_tid(tid)
      retry
    end
  end

end


GC.disable

N_THREAD = 4
#N_THREAD = 2
N_DATA = 5
TX_SIZE = 30
N_TX = 12000/N_THREAD
#N_TX = 1200/N_THREAD
#N_TX = 120/N_THREAD
#TX_SIZE = 5

TX = N_THREAD.times.map do
  N_TX.times.map do
    TX_SIZE.times.map do
      Step.new(rand(2)==0 ? :r : :w, rand(N_DATA))
    end
  end
end
#pp TX


N_DATA.times do |i|
  $db[i] = DataItem.new(i)
end
#pp $db

invoker = InvokeTID.new

threads = []

N_THREAD.times do |i|
  threads << Thread.new(TX[i]) do |tx_list|
    tx_list.each do |tx_seq|
      #p tx_seq
      Transaction.new(tx_seq,invoker,i).run
    end
  end
end

threads.each{|th| th.join}

#pp $db
$db.each do |id,db|
  puts "id=#{id} values=#{db.values.inspect[0..20]}...#{db.values.inspect[-30..-1]} max_tid=#{db.max_tid.inspect[0..20]}...#{db.max_tid.inspect[-30..-1]}"
end
p $n_abort
p $global_tid
