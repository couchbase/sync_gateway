#!/usr/bin/env ruby
# 
####    ./${PKGR} ${PREFIX} ${PREFIXP} ${REVISION} ${GITSPEC} ${PLATFORM} ${ARCH}

require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT         = "couchbase-sync-gateway"
PRODUCT_BASE    = "couchbase"
PRODUCT_KIND    = "sync-gateway"

PREFIX          = ARGV[0] || "/opt/#{PRODUCT}"
PREFIXD         = ARGV[1] || "./opt/#{PRODUCT}"
PRODUCT_VERSION = ARGV[2] || "1.0-1234"
REPO_SHA        = ARGV[3] || "master"
PLATFORM        = ARGV[4] || `uname -s`.chomp + "-" +  `uname -m`.chomp
ARCH            = ARGV[5] ||                           `uname -m`.chomp

RELEASE         = PRODUCT_VERSION.split('-')[0]    # e.g., 1.0
BLDNUM          = PRODUCT_VERSION.split('-')[1]    # e.g., 1234

PKGNAME="#{PRODUCT}_#{RELEASE}-#{BLDNUM}"

START_DIR  = Dir.getwd()
STAGE_DIR = "#{START_DIR}/build/maczip/#{PKGNAME}"
FileUtils.mkdir_p "#{STAGE_DIR}"


Dir.chdir("#{START_DIR}") do
    sh %{tar --directory #{File.dirname(PREFIXD)} -czf "#{PKGNAME}.tar.gz"   #{File.basename(PREFIXD)}}
    FileUtils.cp                                       "#{PKGNAME}.tar.gz", "#{PREFIXD}/#{PKGNAME}.tar.gz"
end




