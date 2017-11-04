Gem::Specification.new do |s|
  s.name          = 'logstash-input-appinsights'
  s.version       = '0.1.0'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'Reads data from Application Insights REST Api.'
  s.homepage      = 'https://github.com/Statoil/logstash'
  s.authors       = ['Statoil']
  s.email         = 'frodehus@outlook.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", '>= 1.60', '<= 2.99'
  s.add_runtime_dependency 'logstash-codec-json'
  s.add_runtime_dependency 'stud', '>= 0.0.22'
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'
  s.add_runtime_dependency 'rest-client'
  s.add_runtime_dependency 'activesupport', '3.2'
end
