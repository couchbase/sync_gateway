#!/usr/bin/env ruby

require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT         = "couchbase-sync-gateway"
PRODUCT_BASE    = "couchbase"
PRODUCT_KIND    = "sync-gateway"

PREFIX          = ARGV[0] || "/opt/#{PRODUCT}"
PREFIXD         = ARGV[1] || "./opt/#{PRODUCT}"
PRODUCT_VERSION = ARGV[2] || "1.0-1234"
PLATFORM        = ARGV[3] || `uname -s`.chomp + "-" +  `uname -m`.chomp

RELEASE         = PRODUCT_VERSION.split('-')[0]    # e.g., 1.0
BLDNUM          = PRODUCT_VERSION.split('-')[1]    # e.g., 1234
PRODUCT_VERSION = "#{RELEASE}.#{BLDNUM}"           #       1.0.1234

sh %{echo "PLATFORM is #{PLATFORM}"}

PKGNAME="#{PRODUCT}_#{RELEASE}"
product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

STARTDIR  = Dir.getwd()
STAGE_DIR = "#{STARTDIR}/build/rpm/#{PKGNAME}.#{BLDNUM}"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/SOURCES"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/BUILD"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/RPMS/i386"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/RPMS/x86_64"

[["#{STARTDIR}", "#{STAGE_DIR}"]].each do |src_dst|
  Dir.chdir(src_dst[0]) do
    ["rpm.spec.tmpl"].each do |x|
      target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
      sh %{sed -e s,@@VERSION@@,#{PRODUCT_VERSION},g #{x}           |
           sed -e s,@@RELEASE@@,#{RELEASE},g                        |
           sed -e s,@@PREFIX@@,#{PREFIX},g                          |
           sed -e s,@@PRODUCT@@,#{PRODUCT},g                        |
           sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g              |
           sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g      |
           sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
      sh %{chmod a+x #{target}}
    end
  end
end

Dir.chdir("#{STAGE_DIR}") do
    sh %{tar --directory #{File.dirname(PREFIXD)} -czf "rpmbuild/SOURCES/#{PRODUCT}_#{PRODUCT_VERSION}.tar.gz" #{File.basename(PREFIXD)}}
    sh %{rpmbuild -bb rpm.spec}
end

