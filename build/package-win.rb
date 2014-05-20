#!/usr/bin/env ruby
# 
#  called by:    build/scripts/jenkins/mobile/build_sync_gateway.bat
#  with params: 
#                PREFIX          e.g.,  opt/couchbase-sync-gateway
#                PREFIXD                ./opt/couchbase-sync-gateway
#                PRODUCT_VERSION        1.0-22
#                REPO_SHA               da9f8142c2fffdffdf7e7f9e5765591e847081a3
#                GITSPEC                stable
#                PLATFORM               windows-x86_64
#                ARCH                   x86_64
#                
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

PKGNAME="setup_#{PRODUCT}_#{RELEASE}-#{BLDNUM}_#{PLATFORM}.exe"
product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

print "\nDEBUG:  0: PREFIX          = ", PREFIX
print "\nDEBUG:  1: PREFIXD         = ", PREFIXD
print "\nDEBUG:  2: PRODUCT_VERSION = ", PRODUCT_VERSION
print "\nDEBUG:  3: REPO_SHA        = ", REPO_SHA
print "\nDEBUG:  4: PLATFORM        = ", PLATFORM
print "\nDEBUG:  5: ARCH            = ", ARCH
print "\n"
print "\nDEBUG:  RELEASE  = ", RELEASE
print "\nDEBUG:  BLDNUM   = ", BLDNUM
print "\n"
print "\nDEBUG:  PKGNAME  = ", PKGNAME
print "\n"

START_DIR  = Dir.getwd()
STAGE_DIR = "#{START_DIR}/opt/couchbase-sync-gateway/"
FileUtils.rm_rf   "#{STAGE_DIR}"
FileUtils.mkdir_p "#{STAGE_DIR}"

[["#{START_DIR}", "#{STAGE_DIR}"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        ["manifest.txt.tmpl", "manifest.xml.tmpl"].each do |x|
            target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
            text = File.read("#{x}")
            text = text.gsub("@@VERSION@@",          "#{BLDNUM}"           )
            text = text.gsub("@@PLATFORM@@",         "#{PLATFORM}"         )
            text = text.gsub("@@RELEASE@@",          "#{RELEASE}"          )
            text = text.gsub("@@REPO_SHA@@",         "#{REPO_SHA}"         )
            text = text.gsub("@@PREFIX@@",           "#{PREFIX}"           )
            text = text.gsub("@@PRODUCT@@",          "#{PRODUCT}"          )
            text = text.gsub("@@PRODUCT_BASE@@",     "#{PRODUCT_BASE}"     )
            text = text.gsub("@@PRODUCT_BASE_CAP@@", "#{product_base_cap}" )
            text = text.gsub("@@PRODUCT_KIND@@",     "#{PRODUCT_KIND}"     )
            File.open("#{target}", "w") { |file| file.puts text }
        end
    end
end

#  make installer

INSTALL_PROJ  = Sync_Gateway.ism
INSTALL_SRC  = "#{START_DIR}/windows/InstallShield_2014_Projects"
INSTALL_OUT  = "#{INSTALL_SRC}/Sync_Gateway/PROJECT_ASSISTANT/SINGLE_EXE_IMAGE/DiskImages/DISK1"

`ISCmdBld.exe #{INSTALL_SRC}/#{INSTALL_PROJ}`

FileUtils.cp  "#{INSTALL_OUT}/setup.exe",  "#{PREFIXD}/#{PKGNAME}"
FileUtils.mv  "#{STAGE_DIR}/manifest.txt", "#{PREFIXD}/manifest.txt"
FileUtils.mv  "#{STAGE_DIR}/manifest.xml", "#{PREFIXD}/manifest.xml"

