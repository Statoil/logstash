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
    config :query, :validate => :string, :required => false, :default => "extend env=tostring(customDimensions.['AspNetCoreEnvironment'])| join kind=leftouter(exceptions|project error_operationId=operation_Id, outerMessage, innermostMessage,problemId, errorType=type)on $left.operation_Id == $right.error_operationId|project-away client_City, client_CountryOrRegion, client_StateOrProvince"
    config :query_interval_hours, :validate => :number, :default => 24, :required => false
    config :base_url, :required => true, :default => "https://api.applicationinsights.io/v1/apps"
    config :interval, :validate => :number, :default => 300
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
        end 
    end 

    def get_ai_data(time)
        begin
            querystring = URI.escape("timespan=#{time}&query=#{@source}|#{get_query}")
            uri = "#{@base_url}/#{@app_id}/query?#{querystring}"
            response = RestClient.get(uri, :'x-api-key' => @key, :accept => "application/json")
            tabledata = JSON[response]
            return tabledata
        rescue JSON::ParserError => e
            @logger.debug("[#{@app_id}] Not a valid json response - assuming no data for specified time range #{time}: #{response}")
        rescue RestClient::ExceptionWithResponse => err
            @logger.error("Got #{err.http_code} for request #{uri}")
        else
            @logger.error("Unexpected error occured")
        end
        return nil
    end

    def process(queue)
        for time in get_time_slices()
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

    def get_time_slices()
        start = get_start_time()
        range = (start.to_i..Time.now.to_i).step(@query_interval_hours.hour)
        slices = Array.new
        range.each do |time|
            slices <<  make_timespan(time)
        end
        return slices
    end

    def make_timespan(timestamp)
         ts = Time.at(timestamp).to_datetime
         start = ts.strftime("%Y-%m-%d %H:%M")
         end_date = (ts + @query_interval_hours.hour).strftime("%Y-%m-%d %H:%M")
         slice = start + "/" + end_date
         return slice
    end
    
    def get_start_time()
        if @last_ts.nil?
            return @start_from_days.days.ago
        end
        
        if @last_ts > Time.now
            start_time = Time.now
        else
            start_time = @last_ts
        end
        return start_time - @query_interval_hours.hour
    end
    
    def get_query()
        if @query.nil?
            query = "order by timestamp asc"
        else
            query = "#{@query} | order by timestamp asc"
        end
        return query
    end

    def stop
    end
end # class LogStash::Inputs::Appinsights
