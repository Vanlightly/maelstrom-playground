#!/usr/bin/env ruby

require_relative 'node.rb'
require 'set'

class Broadcast
    attr_reader :node

    def initialize
        @node = Node.new
        @neighbors = []
        @lock = Mutex.new
        @messages = Set.new

        @node.on "topology" do |msg|
        @neighbors = msg[:body][:topology][@node.node_id]
        @node.log "My neighbors are #{@neighbors.inspect}"
        @node.reply! msg, type: "topology_ok"
        end

        @node.on "read" do |msg|
            @lock.synchronize do
                @node.reply! msg, type: "read_ok", messages: @messages.to_a
            end
        end

        @node.on "broadcast" do |msg|
            # Acknowledge the request
            @node.reply! msg, type: "broadcast_ok"
      
            # Do we need to process this message?
            m = msg[:body][:message]
            new_message = false
            @lock.synchronize do
                unless @messages.include? m
                    @messages.add m
                    new_message = true
                end
            end
      
            if new_message
                # Gossip this message to neighbors
                unacked = @neighbors.dup
                # Except the one who sent it to us; they obviously have the message!
                unacked.delete msg[:src]
      
                # Keep trying until everyone acks
                until unacked.empty?
                    @node.log "Need to replicate #{m} to #{unacked}"
                    unacked.each do |dest|
                        @node.rpc! dest, {type: "broadcast", message: m} do |res|
                            @node.log "Got response #{res}"
                            if res[:body][:type] == "broadcast_ok"
                                # Good, they've got the message!
                                unacked.delete dest
                            end
                        end
                    end
                    # Wait a bit before we try again
                    sleep 1
                end
                @node.log "Done with message #{m}"
            end
        end
    end
end

Broadcast.new.node.main!