#!/usr/bin/env ruby

require 'json'

class Node
    attr_reader :node_id, :node_ids

    def initialize
        @node_id = nil
        @node_ids = nil
        @next_msg_id = 0
        @lock = Monitor.new
        @log_lock = Mutex.new
        @handlers = {}
        @callbacks = {}

        # Register an initial handler for the init message
        on "init" do |msg|
            # Set our node ID and IDs
            @node_id = msg[:body][:node_id]
            @node_ids = msg[:body][:node_ids]
    
            reply! msg, type: "init_ok"
            log "Node #{@node_id} initialized"
        end
    end

    # Register a new message type handler
    def on(type, &handler)
        if @handlers[type]
            raise "Already have a handler for #{type}!"
        end

        @handlers[type] = handler
    end

    # Writes a message to stderr
    def log(message)
        @log_lock.synchronize do
            STDERR.puts message
            STDERR.flush
        end
    end

    # Send a body to the given node id. Fills in src with our own node_id.
    def send!(dest, body)
        msg = { dest: dest,
                src: @node_id,
                body: body}
        @lock.synchronize do
            log "Sent #{msg.inspect}"
            JSON.dump msg, STDOUT
            STDOUT << "\n"
            STDOUT.flush
        end
    end

    # Reply to a request with a response body
    def reply!(req, body)
        body = body.merge({in_reply_to: req[:body][:msg_id]})
        send! req[:src], body
    end

    # Turns a line of STDIN into a message hash
    def parse_msg(line)
        msg = JSON.parse line
        msg.transform_keys!(&:to_sym)
        msg[:body].transform_keys!(&:to_sym)
        msg
    end

    # Send an async RPC request. Invokes block with response message once one
    # arrives.
    def rpc!(dest, body, &handler)
        @lock.synchronize do
            msg_id = @next_msg_id += 1
            @callbacks[msg_id] = handler
            body = body.merge({msg_id: msg_id})
            send! dest, body
        end
    end

    def main!
        Thread.abort_on_exception = true
    
        while line = STDIN.gets
            msg = parse_msg line
            log "Received #{msg.inspect}"
        
            handler = nil
            @lock.synchronize do
                if handler = @callbacks[msg[:body][:in_reply_to]] 
                    @callbacks.delete msg[:body][:in_reply_to]      
                elsif handler = @handlers[msg[:body][:type]]
                else
                    raise "No handler for #{msg.inspect}"
                end
            end
        
            # Actually handle message
            Thread.new(handler, msg) do |handler, msg|
                begin
                    handler.call msg
                rescue => e
                    log "Exception handling #{msg}:\n#{e.full_message}"
                end
            end
        end
    end
end