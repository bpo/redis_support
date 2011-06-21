Gem::Specification.new do |s|
  s.name = %q{redis_support}
  s.version = "0.0.17"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Brian P O'Rourke", "John Le"]
  s.date = %q{2011-01-26}
  s.description = %q{Module for adding redis functionality to classes: simple key namespacing and locking and connections}
  s.email = %q{dolores@doloreslabs.com}
  s.extra_rdoc_files = [
    "README.md"
  ]
  s.files = [
    "lib/redis_support.rb",
    "lib/redis_support/class_extensions.rb",
    "lib/redis_support/key_analysis.rb",
    "lib/redis_support/locks.rb"
  ]
  s.homepage = %q{http://github.com/dolores/redis_support}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{A Redis Support module}
  s.test_files = [
    "test/helper.rb",
    "test/test_redis_support.rb"
  ]

  s.add_dependency(%q<rake>, [">= 0"])
  s.add_dependency(%q<redis>, [">= 1.0.4"])
end

