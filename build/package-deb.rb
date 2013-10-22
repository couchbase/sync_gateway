#!/usr/bin/env ruby

require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT         = "couchbase-sync-gateway"
PRODUCT_BASE    = "couchbase"
PRODUCT_KIND    = "sync-gateway"

PREFIX          = ARGV[0] || "/opt/couchbase"
PREFIXD         = ARGV[1] || "./opt-couchbase-sync-gateway"
PRODUCT_VERSION = ARGV[2] || "1.0-1234"
RELEASE         = PRODUCT_VERSION.split('-')[0]

STARTDIR = Dir.getwd()

product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

STAGE_DIR = "#{STARTDIR}/build/deb/#{PRODUCT}_#{PRODUCT_VERSION}"

FileUtils.rm_rf   "#{STAGE_DIR}"
FileUtils.mkdir_p "#{STAGE_DIR}/opt"
FileUtils.mkdir_p "#{STAGE_DIR}/etc"
FileUtils.mkdir_p "#{STAGE_DIR}/debian"

Dir.glob("*.tmpl").each do |x|
    target = "#{x.gsub('.tmpl', '')}"
    sh %{sed -e s,@@VERSION@@,#{PRODUCT_VERSION},g #{x}         |
         sed -e s,@@RELEASE@@,#{RELEASE},g                      |
         sed -e s,@@PREFIX@@,#{PREFIX},g                        |
         sed -e s,@@PRODUCT@@,#{PRODUCT},g                      |
         sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g            |
         sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g    |
         sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
    sh %{chmod a+x #{target}}
end

sh %{cp -R #{PREFIX} #{STAGE_DIR}/opt}

Dir.chdir STAGE_DIR do
  sh %{dch -b -v #{PRODUCT_VERSION} "Released debian package for version #{PRODUCT_VERSION}"}
  sh %{dpkg-buildpackage -b}
end

