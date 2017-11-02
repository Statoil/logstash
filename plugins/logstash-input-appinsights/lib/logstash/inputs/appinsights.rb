# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "rest-client"

class LogStash::Inputs::Appinsights < LogStash::Inputs::Base
  config_name "appinsights"

  default :codec, "json"

  config :apps, :validate => :hash, :required => true

  config :interval, :validate => :number, :default => 60

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
          response = RestClient.get("https://api.applicationinsights.io/v1/apps/#{app_id}/query?query=#{source}%7C%20where%20timestamp%20%3E%3D%20ago(5m)", :'x-api-key' => api_key)
          tabledata = JSON[response]
          parse_ai_data(appName, tabledata, queue)
        end
      end
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def parse_ai_data(appName, data, queue)
   if data.nil?
     return
   end
   tables = data["tables"]
   tables.each do |table|
     cols = table["columns"]
     rows = table["rows"]
     rows.each do |row|
       e = parse_event(row, cols)
       next if e.nil?
       e["application"] = appName
       @codec.decode(e.to_json()) do |event|
         decorate(event)
         event.set("@timestamp", LogStash::Timestamp.new(Time.parse(e["timestamp"])))
         queue << event
       end
     end
   end
  end

  def parse_event(row, cols)
   if cols.nil?
     return nil
   end
   e = Hash.new()
   index = 0
   cols.each do |col|
     e[col["name"]] = row[index]
     index += 1
   end
   return e
  end

  def stop
  end
end # class LogStash::Inputs::Appinsights
