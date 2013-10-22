#!/usr/bin/env ruby

require 'rubygems'
require 'fileutils'
require 'rake'
require 'util'

PREFIX       = ARGV[0] || "/opt/couchbase"
PRODUCT      = ARGV[1] || "couchbase-sync-gateway"
PRODUCT_BASE = ARGV[2] || "couchbase"
PRODUCT_KIND = ARGV[3] || "sync-gateway"

if ENV["PRODUCT_VERSION"] != nil
   PRODUCT_GIT            = ENV["PRODUCT_VERSION"]
else
    PRODUCT_GIT            = `#{bin('git')} describe`.chomp
end
PRODUCT_VERSION        = PRODUCT_GIT + '-' + os() + "." + `uname -m`.chomp
PRODUCT_VERSION_PREFIX = PRODUCT_VERSION.split('-')[0]

product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

ver = PRODUCT_VERSION_PREFIX
ver = "1.0~" + ver unless ver.match(/^[0-9]/) # Packager wants a number prefix.

REL = PRODUCT_GIT.split('-')[1] || 1

scan_check(PREFIX)

FileUtils.mkdir_p "#{ENV['HOME']}/macbuild/BUILD"

sh %{tar --directory #{File.dirname(PREFIX)} -czf package-#{PRODUCT_BASE}.tar.gz #{File.basename(PREFIX)}}

FileUtils.cp "package-#{PRODUCT_BASE}.tar.gz", "#{ENV['HOME']}/macbuild/BUILD/#{PRODUCT}_#{ver}.tar.gz"



