# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "rest-client"
require "active_support/all"
require "uri"

class LogStash::Inputs::Appinsights < LogStash::Inputs::Base
    config_name "appinsights"
    default :codec, "json"

    config :source, :validate => :string, :required => true
    config :key, :validate => :string, :required => true
    config :app_id, :validate => :string, :required => true
    config :query, :validate => :string, :required => false
    config :base_url, :required => true, :default => "https://api.applicationinsights.io/v1/apps"
    config :interval, :validate => :number, :default => 60
    config :start_from_days, :validate => :number, :default => 1
    config :sincedb_path, :validate => :string, :required => false
    
    last_ts = nil

    public
    def register
        @host = Socket.gethostname
    end

    def run(queue)
        while !stop?
            process(queue)
        Stud.stoppable_sleep(@interval) { stop? }
        end # loop
    end # def run

    def get_ai_data(time)
        query = (@query || "order by timestamp asc")
	    uri = "#{@base_url}/#{@app_id}/query?timespan=#{time}&query=#{@source}|#{query}|order by timestamp asc"
        response = RestClient.get(URI.escape(uri), :'x-api-key' => @key, :accept => "application/json")
        tabledata = JSON[response]
        return tabledata
    end

    def process(queue)
        for time in get_time_slices(@last_ts || @start_from_days.days.ago)
           data = get_ai_data(time)
           parse_data(data, queue)
        end
    end

    def parse_data(data, queue)
        if data.nil?
            return
        end
        tables = data["tables"]
        if tables.nil?
            return
        end

        tables.each do |table|
            cols = table["columns"]
            rows = table["rows"]
            ts = nil
            rows.each do |row|
                e = parse_row(row, cols)
                next if e.nil?
                @last_ts = Time.parse(e["timestamp"])
                @codec.decode(e.to_json()) do |event|
                   	decorate(event)
                   	event.set("@timestamp", LogStash::Timestamp.new(@last_ts))
                   	queue << event
			    end
            end
        end
    end

    def parse_row(row, cols)
        if cols.nil?
            return nil
        end

        e = Hash.new
        index = 0
        cols.each do |col|
             e[col["name"]] = row[index]
             index += 1
        end
        return e
    end

    def get_time_slices(start)
        range = (start.to_i..Time.now.to_i).step(1.hour)
        slices = Array.new
        range.each do |time|
            slices <<  make_timespan(time)
        end
        return slices
    end

    def make_timespan(timestamp)
         ts = Time.at(timestamp).to_datetime
         start = ts.strftime("%Y-%m-%d %H:%M")
         end_date = (ts + 1.hour).strftime("%Y-%m-%d %H:%M")
         slice = start + "/" + end_date
         return slice
    end

    def stop
    end
end # class LogStash::Inputs::Appinsights
