require 'rubygems'

require 'redis'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "/"))
require 'redis_support/class_extensions'
require 'redis_support/locks'

module RedisSupport
  # Inspired/take from the redis= in Resque
  #
  # Accepts:
  #   1. A 'hostname:port' string
  #   2. A 'hostname:port:db' string (to select the Redis db)
  #   3. An instance of `Redis`, `Redis::Client`
  def self.redis=(connection)
    if connection.respond_to? :split
      host, port, db = connection.split(':')
      @redis = Redis.new(:host => host,:port => port,:thread_safe => true,:db => db)
    else
      @redis = connection
    end
  end

  def self.redis
    @redis
  end

  def redis=(redis)
    @redis = redis
  end

  def redis
    @redis || RedisSupport.redis
  end

  def keys
    Keys
  end

  module Keys ; end

  def self.included(model)
    model.extend ClassMethods
    model.extend RedisSupport
  end
end
