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
#                PRODUCT_KIND           sync-gateway
#                
require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT_BASE    = "couchbase"

PREFIX          = ARGV[0] || "/opt/couchbase-sync-gateway"
PREFIXD         = ARGV[1] || "./opt/couchbase-sync-gateway"
PRODUCT_VERSION = ARGV[2] || "0.0.0-1234"
REPO_SHA        = ARGV[3] || "master"
PLATFORM        = ARGV[4] || 'windows-x64'
ARCH            = ARGV[5] || 'x64'
PRODUCT_KIND    = ARGV[6] || "sync-gateway"

PRODUCT         = "#{PRODUCT_BASE}-#{PRODUCT_KIND}"
RELEASE         = PRODUCT_VERSION.split('-')[0]    # e.g., 1.0.0
BLDNUM          = PRODUCT_VERSION.split('-')[1]    # e.g., 1234

PKGNAME="setup_#{PRODUCT}_#{RELEASE}-#{BLDNUM}_#{ARCH}.exe"
product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

print "\nDEBUG:  0: PREFIX          = ", PREFIX
print "\nDEBUG:  1: PREFIXD         = ", PREFIXD
print "\nDEBUG:  2: PRODUCT_VERSION = ", PRODUCT_VERSION
print "\nDEBUG:  3: REPO_SHA        = ", REPO_SHA
print "\nDEBUG:  4: PLATFORM        = ", PLATFORM
print "\nDEBUG:  5: ARCH            = ", ARCH
print "\nDEBUG:  6: PRODUCT_KIND    = ", PRODUCT_KIND
print "\n"
print "\nDEBUG:  RELEASE  = ", RELEASE
print "\nDEBUG:  BLDNUM   = ", BLDNUM
print "\n"
print "\nDEBUG:  PKGNAME  = ", PKGNAME
print "\nDEBUG:  PRODUCT  = ", PRODUCT
print "\n"

START_DIR  = Dir.getwd()
STAGE_DIR = "#{START_DIR}/opt/couchbase-sync-gateway/"
                                                     #  created by build_sync_gateway.bat
[["#{START_DIR}", "#{STAGE_DIR}"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        ["manifest.txt.tmpl", "manifest.xml.tmpl"].each do |x|
            target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
            text = File.read("#{x}")
            text.gsub("@@VERSION@@",          "#{RELEASE}-#{BLDNUM}")
            text.gsub("@@PLATFORM@@",         "#{PLATFORM}"         )
            text.gsub("@@RELEASE@@",          "#{RELEASE}"          )
            text.gsub("@@REPO_SHA@@",         "#{REPO_SHA}"         )
            text.gsub("@@PREFIX@@",           "#{PREFIX}"           )
            text.gsub("@@PRODUCT@@",          "#{PRODUCT}"          )
            text.gsub("@@PRODUCT_BASE@@",     "#{PRODUCT_BASE}"     )
            text.gsub("@@PRODUCT_BASE_CAP@@", "#{product_base_cap}" )
            text.gsub("@@PRODUCT_KIND@@",     "#{PRODUCT_KIND}"     )
            File.open("#{target}", "w") { |file| file.puts text }
        end
    end
end

#  make installer

INSTALL_PROJ  = "#{PRODUCT_KIND}.ism"
INSTALL_SRC   = "#{START_DIR}/windows/InstallShield_2014_Projects"
INSTALL_OUT   = "#{INSTALL_SRC}/#{PRODUCT_KIND}/SINGLE_EXE_IMAGE/Release/DiskImages/DISK1"

proj_param    = "#{INSTALL_SRC}/#{INSTALL_PROJ}"
proj_param    = proj_param.gsub('/', '\\')

path_to_workspace = "#{ENV['WORKSPACE']}"
path_to_sgw_files = "#{path_to_workspace}\\godeps\\src\\github.com\\couchbase\\sync_gateway\\build\\opt\\#{PRODUCT}"
installer_params  = "-l PATH_TO_JENKINS_WORKSPACE=#{path_to_workspace} -l PATH_TO_SYNC_GATEWAY_FILES=#{path_to_sgw_files}"

print "\nISCmdBld.exe -v -y #{RELEASE} -d ProductVersion=#{RELEASE}.#{BLDNUM} #{installer_params} -p #{proj_param}\n"
print   `ISCmdBld.exe -v -y #{RELEASE} -d ProductVersion=#{RELEASE}.#{BLDNUM} #{installer_params} -p #{proj_param}`
print "\n\n"

if  File.exists?("#{INSTALL_OUT}/setup.exe")
    FileUtils.cp "#{INSTALL_OUT}/setup.exe",  "#{PREFIXD}/#{PKGNAME}"
    FileUtils.mv "#{STAGE_DIR}/manifest.txt", "#{PREFIXD}/manifest.txt"
    FileUtils.mv "#{STAGE_DIR}/manifest.xml", "#{PREFIXD}/manifest.xml"
else
    print "\nFAIL:  File does not exist: #{INSTALL_OUT}/setup.exe"
    exit 99
end
