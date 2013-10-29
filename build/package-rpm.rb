#!/usr/bin/env ruby
# 
####    ./${PKGR} ${PREFIX} ${PREFIXP} ${REVISION} ${GITSPEC} ${PLATFORM}

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
ARCH            =                                      `uname -m`.chomp

RELEASE         = PRODUCT_VERSION.split('-')[0]    # e.g., 1.0
BLDNUM          = PRODUCT_VERSION.split('-')[1]    # e.g., 1234

PKGNAME="#{PRODUCT}_#{RELEASE}-#{BLDNUM}"
product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

START_DIR  = Dir.getwd()
STAGE_DIR = "#{START_DIR}/build/rpm/#{PKGNAME}"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/SOURCES"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/BUILD"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/BUILDROOT"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/RPMS/i386"
FileUtils.mkdir_p "#{STAGE_DIR}/rpmbuild/RPMS/x86_64"

# MANIFEST_DIR="#{STAGE_DIR}/rpmbuild/BUILDROOT/#{PRODUCT}-#{BLDNUM}-#{RELEASE}.#{ARCH}/opt/#{PRODUCT}"
# FileUtils.mkdir_p "#{MANIFEST_DIR}

[["#{START_DIR}", "#{STAGE_DIR}"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        ["rpm.spec.tmpl", "manifest.xml.tmpl"].each do |x|
            target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
            sh %{sed -e s,@@VERSION@@,#{BLDNUM},g                    #{x} |
                 sed -e s,@@PLATFORM@@,#{PLATFORM},g                      |
                 sed -e s,@@RELEASE@@,#{RELEASE},g                        |
                 sed -e s,@@REPO_SHA@@,#{REPO_SHA},g                      |
                 sed -e s,@@PREFIX@@,#{PREFIX},g                          |
                 sed -e s,@@PRODUCT@@,#{PRODUCT},g                        |
                 sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g              |
                 sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g      |
                 sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
            sh %{chmod a+x #{target}}
        end
    end
end
# FileUtils.mv  "#{STAGE_DIR}/manifest.xml", "#{MANIFEST_DIR}/manifest.xml"
FileUtils.mv  "#{STAGE_DIR}/manifest.xml", "#{PREFIXD}/manifest.xml"

Dir.chdir("#{START_DIR}") do
    sh %{tar --directory #{File.dirname(PREFIXD)} -czf "#{STAGE_DIR}/rpmbuild/SOURCES/#{PRODUCT}_#{BLDNUM}.tar.gz" #{File.basename(PREFIXD)}}
end
Dir.chdir("#{STAGE_DIR}") do
    sh %{rpmbuild -bb rpm.spec}
end

FileUtils.cp "#{STAGE_DIR}/rpmbuild/RPMS/#{ARCH}/#{PRODUCT}-#{BLDNUM}-#{RELEASE}.#{ARCH}.rpm", "#{PREFIXD}/#{PRODUCT}_#{PRODUCT_VERSION}_#{ARCH}.rpm"
