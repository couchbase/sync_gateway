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

[["#{PRODUCT_KIND}", "."]].each do |src_dst|
  Dir.chdir(src_dst[0]) do
    ["rpm.spec.tmpl"].each do |x|
      target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
      sh %{sed -e s,@@VERSION@@,#{ver},g #{x} |
           sed -e s,@@RELEASE@@,#{REL},g |
           sed -e s,@@PREFIX@@,#{PREFIX},g |
           sed -e s,@@PRODUCT@@,#{PRODUCT},g |
           sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g |
           sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g |
           sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
      sh %{chmod a+x #{target}}
    end
  end
end

FileUtils.mkdir_p "#{ENV['HOME']}/rpmbuild/SOURCES"
FileUtils.mkdir_p "#{ENV['HOME']}/rpmbuild/BUILD"
FileUtils.mkdir_p "#{ENV['HOME']}/rpmbuild/RPMS/i386"
FileUtils.mkdir_p "#{ENV['HOME']}/rpmbuild/RPMS/x86_64"

sh %{tar --directory #{File.dirname(PREFIX)} -czf package-#{PRODUCT_BASE}.tar.gz #{File.basename(PREFIX)}}

FileUtils.cp "package-#{PRODUCT_BASE}.tar.gz", "#{ENV['HOME']}/rpmbuild/SOURCES/#{PRODUCT}_#{ver}.tar.gz"

sh %{rpmbuild -bb #{PRODUCT_KIND}/rpm.spec}

FileUtils.rm_f "rpm.spec"
FileUtils.rm_f "~/rpmbuild/SOURCES/#{PRODUCT}_#{ver}.tar.gz"


