#!/usr/bin/env ruby

require 'rubygems'
require 'fileutils'
require 'rake'

PREFIX       = ARGV[0] || "/opt/couchbase"
PRODUCT      = ARGV[1] || "couchbase-sync-gateway"
PRODUCT_BASE = ARGV[2] || "couchbase"
PRODUCT_KIND = ARGV[3] || "sync-gateway"

STARTDIR = Dir.getwd()

if ENV["PRODUCT_VERSION"] != nil
   PRODUCT_GIT            = ENV["PRODUCT_VERSION"]
else
    PRODUCT_GIT            = `#{'git'} describe`.chomp
end

PRODUCT_VERSION        = PRODUCT_GIT + '-' + `uname -s`.chomp.downcase + "." + `uname -m`.chomp
PRODUCT_VERSION_PREFIX = PRODUCT_VERSION.split('-')[0]

product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

ver = PRODUCT_VERSION_PREFIX
ver = "1.0~" + ver unless ver.match(/^[0-9]/) # dpkg-deb/deb-version wants a number prefix.

REL = PRODUCT_GIT.split('-')[1] || 1

STAGE_DIR = "#{STARTDIR}/build/deb/#{PRODUCT}_#{ver}-#{REL}"

FileUtils.rm_rf STAGE_DIR

FileUtils.mkdir_p "#{STAGE_DIR}/opt"
FileUtils.mkdir_p "#{STAGE_DIR}/etc"
FileUtils.mkdir_p "#{STAGE_DIR}/debian"

[["#{PRODUCT_KIND}", "#{STAGE_DIR}/debian"]].each do |src_dst|
  Dir.chdir(src_dst[0]) do
    Dir.glob("*.tmpl").each do |x|
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

sh %{cp -R #{PREFIX} #{STAGE_DIR}/opt}

Dir.chdir STAGE_DIR do
  sh %{dch -b -v #{ver} "Released debian package for version #{ver}"}
  sh %{dpkg-buildpackage -b}
end


