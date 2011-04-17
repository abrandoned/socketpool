#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'socket'
require 'thread'

class SocketPool

  attr_accessor :host, :port, :size, :timeout, :checked_out

  # Create a new socket pool
  #
  def initialize(host, port, opts={})
    @host, @port = host, port

    # Pool size and timeout.
    @size      = opts[:size] || 2
    @timeout   = opts[:timeout]   || 5.0
    @eager      = opts[:eager] || false

    # Mutex for synchronizing pool access
    @connection_mutex = Mutex.new

    # Condition variable for signal and wait
    @queue = ConditionVariable.new
    
    @socktype     = opts[:type] || :tcp
    @sockopts     = opts[:socketopts].nil? ? [] : [opts[:socketopts]].flatten.inject([]){|s, so| s << so}
    @sockets      = []
    @pids         = {}
    @checked_out  = []
    
    initialize_socketpool if @eager
  end
  
  def close
    @sockets.each do |sock|
      begin
        sock.close
      rescue IOError => ex
        warn "IOError when attempting to close socket connected to #{@host}:#{@port}: #{ex.inspect}"
      end
    end
    @host = @port = nil
    @sockets.clear
    @pids.clear
    @checked_out.clear
  end
  
  # Return a socket to the pool.
  # Allow for destroying a resetting socket if the application determines the connection is no good
  def checkin(socket, reset=false)
    @connection_mutex.synchronize do
      if reset
        @socket.delete(socket)
        @checked_out.delete(socket)
        checkin(checkout_new_socket, false)
      else
        @checked_out.delete(socket)          
        @queue.signal
      end
    end
    true
  end  

  # Adds a new socket to the pool and checks it out.
  #
  # This method is called exclusively from #checkout;
  # therefore, it runs within a mutex.
  def checkout_new_socket
    begin
    socket = Socket.new(so_domain(@socktype), so_type(@socktype), 0)
    @sockaddr ||= Socket.pack_sockaddr_in(@port, @host) if ![:unix, :unigram].include?(@socktype)
    @sockaddr ||= Socket.pack_sockaddr_un(@host) if [:unix, :unigram].include?(@socktype)
    socket.connect(@sockaddr)
    if @sockopts.size > 0
      @sockopts.each{ |opt| socket.setsockopt(opt[:level], opt[:optname], opt[:optval]) }  
    end
    rescue => ex
      raise ConnectionFailure, "Failed to connect to host #{@host} and port #{@port}: #{ex}"
    end

    @checked_out << socket
    @sockets << socket
    @pids[socket] = Process.pid
    socket
  end

  # Checks out the first available socket from the pool.
  #
  # If the pid has changed, remove the socket and check out
  # new one.
  #
  # This method is called exclusively from #checkout;
  # therefore, it runs within a mutex.
  def checkout_existing_socket
    socket = (@sockets - @checked_out).first
    if @pids[socket] != Process.pid
       @pids[socket] = nil
       @sockets.delete(socket)
       socket.close
       checkout_new_socket
    else
      @checked_out << socket
      socket
    end
  end

  # Check out an existing socket or create a new socket if the maximum
  # pool size has not been exceeded. Otherwise, wait for the next
  # available socket.
  def checkout
    start_time = Time.now
    loop do
      if (Time.now - start_time) > @timeout
          raise ConnectionTimeoutError, "could not obtain connection within " +
            "#{@timeout} seconds. The max pool size is currently #{@size}; " +
            "consider increasing the pool size or timeout."
      end

      @connection_mutex.synchronize do
        socket = if @checked_out.size < @sockets.size
                   checkout_existing_socket
                 elsif @sockets.size < @size
                   checkout_new_socket
                 end

        if socket
          return socket
        else
          # Otherwise, wait
          @queue.wait(@connection_mutex)
        end
      end
    end
  end
  
  def so_type(val)
    val = val.downcase.to_sym if val.respond_to?(:downcase) && val.respond_to?(:to_sym)
    @so_type ||= {
      :tcp => Socket::SOCK_STREAM,
      :tcp6 => Socket::SOCK_STREAM,
      :udp => Socket::SOCK_DGRAM,
      :udp6 => Socket::SOCK_DGRAM,
      :unix => Socket::SOCK_STREAM,
      :unigram => Socket::SOCK_DGRAM
    }[val]
  end
  
  def so_domain(val)
    val = val.downcase.to_sym if val.respond_to?(:downcase) && val.respond_to?(:to_sym)
    @so_domain ||= {
      :tcp => Socket::AF_INET,
      :tcp6 => Socket::AF_INET6,
      :udp => Socket::AF_INET,
      :udp6 => Socket::AF_INET6,
      :unix => Socket::AF_UNIX,
      :unigram => Socket::AF_UNIX
    }[val]
  end

  private 
  
  def initialize_socketpool
    begin
      @size.times{ checkout_new_socket }
    ensure
      @checked_out = []
    end
  end
end
