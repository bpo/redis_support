#
# Matcher represents a single registered key. It understands that key
# names can have partition identifiers and finds the identifier
# position in the key.
#
# The match is rather strict, so have all your
# keys registered.
class Matcher
  attr_accessor :regex

  class << self; attr_accessor :partition_key; end
  
  def initialize(struct)
    @regex = Regexp.new("^" + struct.gsub( /[A-Z]+(_[A-Z]+)*/, "([^:]+)" ) + "$")
    @partition_key_position = self.class.partition_key &&
      @regex.match(struct).to_a.find_index {|k| k == self.class.partition_key }
  end

  def match( s )
    @last_match = @regex.match( s )
  end

  def partition_value
    @partition_key_position && @last_match && @last_match[@partition_key_position]
  end
end

# monkey patches for convenience
class Array
  def sum
    reduce(0) { |a, x| a += x }
  end
end


module RedisAnalysis
  ALL_KEYS = "analysis:keys"
  REGISTERED_KEYS = "analysis:registered"
  OTHER_KEYS = "analysis:unregistered"
  KEY_SIZES = "analysis:key_sizes"
  DETAILED_KEY_SIZES = "analysis:key_sizes"
  KEY_GROUPS = "analysis:key_groups"
  KEY_PENDING_DELETION = "analysis:keys_pending_deletion"

  # Timeout is high here to allow all keys to be gathered.
  def patient
    @patient ||= Redis.new :port => 9379, :timeout => 60
  end

  # Use the standard 9379 db for analysis, DB id 2
  def analysis
    @analysis ||= Redis.new :port => 9380
  end

  def clear_analysis
    analysis.flushdb
  end

  # Read the all keys and put them in the right buckets. All buckets are listed in KEY_GROUPS set.
  # Estimate sizes by default. We estimate size by key and by group in two redis hashes.
  #
  # Most likely you'd want to clear_analysis before you run this
  #
  # On a modest database, this should take ~ 20 min.
  #
  def load_db( estimate_size=true )
    analysis.sadd KEY_GROUPS, OTHER_KEYS
    patient.keys("*").each do |key|
      analysis.rpush ALL_KEYS, key
      if( matcher = Support.match?(key) )
        puts "matched: #{key} with #{matcher}" unless @chatty
        group = "#{REGISTERED_KEYS}:#{matcher}"
        analysis.rpush group, key
        analysis.sadd KEY_GROUPS, group
        if estimate_size
          size = key_size( patient, key )
          analysis.hincrby( KEY_SIZES, group, size )
          analysis.hincrby( DETAILED_KEY_SIZES, key, size )
        end
      else
        puts "unmatched: #{key}" unless @chatty
        analysis.rpush OTHER_KEYS, key
        if estimate_size
          size = key_size( patient, key )
          analysis.hincrby( KEY_SIZES, OTHER_KEYS, size )
          analysis.hincrby( DETAILED_KEY_SIZES, key, size )
        end
      end
    end
  end

  def sanity
    patient.info["db0"] =~ /keys=(.*),.*/
    expected = $1.to_i
    actual = analysis.llen(ALL_KEYS)
    if expected != actual
      raise "Expected #{expected} entries but found #{actual}"
    end
  end

  def debug
    puts "Patient:"
    pp patient.info

    puts
    puts "Analysis:"
    pp analysis.info
  end

  def write_full_size_report( file )
    file.write(FasterCSV.generate_line(["key","count","size"]))
    analysis.smembers( KEY_GROUPS ).map do |group_key|
      [analysis.llen( group_key ), analysis.hget( KEY_SIZES, group_key), group_key]
    end.sort.each do |count, size, group_key|
      actual_key = group_key.gsub( /^analysis:registered:/, "")
      file.write( FasterCSV.generate_line([ actual_key , count, size ]) )
    end
  end

  #Estimate 'key' size stored in 'redis'
  def key_size( redis, key )
    case redis.type(key)
    when "none" then 0
    when "string" then redis.get(key).size
    when "list" then redis.lrange(key,0,-1).map{|m| 1+m.size}.sum || 1
    when "zset" then redis.zrange(key,0,-1).map{|m| 1+m.size}.sum || 1
    when "set" then redis.smembers(key).map{|m| 1+m.size}.sum || 1
    when "hash" then redis.hgetall(key).to_a.flatten.map{|m| 1+m.size}.sum || 1
    end
  end


  #
  # Create a detailed report with breakdown by partition key value. This is a great start for pivotal table in excel
  #
  def write_report_by_partition_key( file, do_size=true )
    file.write(FasterCSV.generate_line(["partition key","partition value","count", "size"] + partition_info_header))
    analysis.smembers( KEY_GROUPS ).map do |key|
      [analysis.llen(key), key]
    end.sort.each do |value,key|
      actual_key = key.gsub( /^analysis:registered:/, "")
      group_by_partition_key( actual_key, file, do_size )
    end
  end

  # Group individual keys within the key group by partition key value
  def group_by_partition_key( key, file, do_size=false )
    matcher = Support::who_matches?( key )
    return unless matcher
    partition_counts = {}
    partition_counts.default = 0
    partition_sizes = {}
    partition_sizes.default = 0
    analysis.lrange( "#{REGISTERED_KEYS}:#{key}", 0, -1 ).each do |original_key|
      matcher.match( original_key )
      partition_value = matcher.partition_value || 'no_partition_value'
      partition_counts[partition_value] += 1
      partition_sizes[partition_value] += analysis.hget( KEY_SIZES, original_key ).to_f if do_size
    end
    partition_counts.each_pair.sort {|(k,v),(k2,v2)| v <=> v2}.each do |partition_value, key_count|
      extras = partition_info( partition_value ) || []
      file.write( FasterCSV.generate_line( [ key, partition_value, key_count, do_size ? partition_sizes[partition_value] : 0] | extras) )
    end
  end

  # Override these if you want to add extra information to the report.
  def partition_info_header
    []
  end

  def partition_info( partition_value )
    []
  end

  #
  # Hack this function to look at the keys that aren't registered.
  #
  def scan_stray_keys
    leftovers = []
    num_stray = analysis.llen( OTHER_KEYS )
    (0..num_stray).to_a.each_slice(1000) do |slice|
      p slice.first
      ok = analysis.lrange( OTHER_KEYS, slice.first, slice.last)
      keys = ok.map do |k|
        if k =~ /^lock/
          'lock'
        elsif k =~ /^resque:/
          'resque'
        else
          k.split(':')[-1]
        end
      end
      leftovers << keys
    end

    leftovers.flatten.counts.each_pair do |k,v|
      puts "#{k}\n#{v}"
    end
  end


  module Support
    def self.keys
      RedisSupport::Keys
    end

    #TODO setup matcher partition key
    def self.matchers
      return @matchers if @matchers
      @matchers = YAML::Omap.new
      Support.keys.keystructs.sort {|a,b| b.size <=> a.size }.each do |struct|
        @matchers[Matcher.new(struct)] = struct 
      end 
      @matchers
    end

    def self.reset_matchers
      @matchers = nil
    end

    def self.match?( key )
      m = matchers.find {|m| m.first.match key }
      m && m.last
    end

    def self.who_matches?( key )
      match = matchers.find {|m| m.first.match key }
      match && match.first
    end
  end
end


#Class to hold all keys that aren't yet registered
class GuestKeys
  include RedisSupport
end

def define_guest_keys
  GuestKeys.instance_eval <<KEYS
    redis_key :resque_jobs, "resque:QUEUE_NAME"
KEYS
end

def run_redis_analysis
  #############
  # This stuff is pretty much kid-safe.
  # Make sure you DON'T run it IN PRODUCTION. I.e. analysis server should NOT be PRODUCTION. It won't crash it, but it will be bad
  #############

  include RedisAnalysis
  define_guest_keys

  puts "Starting analysis"
  clear_analysis
  analysis_time = Benchmark.measure { load_db }
  p analysis_time

  puts "Writing size report"
  #Generate the report with count and total size per key group
  File.open( "redis_analysis_w_size.csv", "w" ) do |report|
    full_size_time = Benchmark.measure { write_full_size_report(report) }
  end

  puts "Writing report by partition key"
  #Generate the report with detailed breakdown by  count and total size per key group
  File.open( "redis_analysis_by_state.csv", "w" ) do |detailed_report|
    write_report_by_job_id( detailed_report )
  end

  puts "Done"

end
