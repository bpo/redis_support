# -*- ruby -*-
require 'rubygems'
require 'bundler'
Bundler.setup

require 'rake'

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "redis_support #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :analyze do
  $LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "lib"))
  dir = File.join(File.dirname(__FILE__), "analysis")

  `redis-server #{dir}/redis-patient.conf`
  `redis-server #{dir}/redis-analysis.conf`

  require 'irb'
  require 'pp'
  require 'benchmark'
  require 'redis_support'
  require 'redis_support/key_analysis'
  include RedisAnalysis

  at_exit do
    puts "Killing patient and analysis redis servers..."
    analysis_pid = `ps -A -o pid,command | grep [r]edis-analysis`.split(" ")[0]
    patient_pid = `ps -A -o pid,command | grep [r]edis-patient`.split(" ")[0]
    Process.kill("TERM", analysis_pid.to_i)
    Process.kill("KILL", patient_pid.to_i)
  end

  puts "To run the actual analysis run: "
  puts "Define the keys that are being used (override the define_guest_keys method)"
  puts "Run: `run_redis_analysis`"

  ARGV.clear
  IRB.start
end

task :default => :test
