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

  config :apps, :validate => :hash, :required => true
  config :base_url, :default => "https://api.applicationinsights.io/v1/apps"

  config :interval, :validate => :number, :default => 60
  config :start_from_days, :validate => :number, :default => 1
  public
  def register
    @host = Socket.gethostname
  end

  def run(queue)
    while !stop?
      @apps.keys.each do |appName|
        begin
          api_key = @apps[appName]["key"]
          app_id = @apps[appName]["id"]
          source = @apps[appName]["source"]
	  process(app_id, api_key, source, appName, queue)
        end
      end
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def get_ai_data(app_id, key, source, time)
	uri = "#{@base_url}/#{app_id}/query?timespan=#{time}&query=#{source}| order by timestamp asc"
	@logger.error(uri)
	@logger.error(URI.escape(uri))
        response = RestClient.get(URI.escape(uri), :'x-api-key' => key, :accept => "application/json")
        tabledata = JSON[response]
        return tabledata
  end

  def process(app_id, key, source, appName, queue)
        for time in get_time_slices(@start_from_days.days.ago)
                start = time.strftime("%Y-%m-%d %H:%M")
                end_date = (time + 1.hour).strftime("%Y-%m-%d %H:%M")
                data = get_ai_data(app_id, key, source, start + "/" + end_date)
                parse_data(data, appName, queue)
        end
  end

  def parse_data(data, appName)
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
                        ts = Time.parse(e["timestamp"])
                        e["application"] = appName
                        @codec.decode(e.to_json()) do |event|
                        	decorate(event)
                        	event.set("@timestamp", LogStash::Timestamp.new(ts))
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
                slices <<  Time.at(time).to_datetime
        end
        return slices
  end


  def stop
  end
end # class LogStash::Inputs::Appinsights
