#!/usr/bin/env ruby
# 
####    ./${PKGR} ${PREFIX} ${PREFIXP} ${REVISION} ${GITSPEC} ${PLATFORM} ${ARCH}

require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT         = "couchbase-sync-gateway"
PRODUCT_BASE    = "couchbase"

PREFIX          = ARGV[0] || "/opt/#{PRODUCT}"
PREFIXD         = ARGV[1] || "./opt/#{PRODUCT}"
PRODUCT_VERSION = ARGV[2] || "1.0-1234"
REPO_SHA        = ARGV[3] || "master"
PLATFORM        = ARGV[4] || `uname -s`.chomp + "-" +  `uname -m`.chomp
ARCH            = ARGV[5] ||                           `uname -m`.chomp
PRODUCT_KIND    = ARGV[6] || "sync-gateway"
PRODUCT_EXEC    = ARGV[7] || "sync_gateway"

PRODUCT         = "#{PRODUCT_BASE}-#{PRODUCT_KIND}"
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


[["#{START_DIR}", "#{STAGE_DIR}"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        ["rpm.spec.tmpl", "manifest.txt.tmpl", "manifest.xml.tmpl"].each do |x|
            target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
            sh %{sed -e s,@@VERSION@@,#{RELEASE},g                   #{x} |
                 sed -e s,@@PLATFORM@@,#{PLATFORM},g                      |
                 sed -e s,@@RELEASE@@,#{BLDNUM},g                         |
                 sed -e s,@@REPO_SHA@@,#{REPO_SHA},g                      |
                 sed -e s,@@PREFIX@@,#{PREFIX},g                          |
                 sed -e s,@@PRODUCT@@,#{PRODUCT},g                        |
                 sed -e s,@@PRODUCT_EXEC@@,#{PRODUCT_EXEC},g              |
                 sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g              |
                 sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g      |
                 sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
            sh %{chmod a+x #{target}}
        end
    end
end
FileUtils.mv  "#{STAGE_DIR}/manifest.txt", "#{PREFIXD}/manifest.txt"
FileUtils.mv  "#{STAGE_DIR}/manifest.xml", "#{PREFIXD}/manifest.xml"

Dir.chdir("#{START_DIR}") do
    sh %{tar --directory #{File.dirname(PREFIXD)} -czf "#{STAGE_DIR}/rpmbuild/SOURCES/#{PRODUCT}_#{RELEASE}.tar.gz" #{File.basename(PREFIXD)}}
end
Dir.chdir("#{STAGE_DIR}") do
    sh %{rpmbuild -bb --define "_binary_filedigest_algorithm  1"  --define "_binary_payload 1" rpm.spec}
end

FileUtils.cp "#{STAGE_DIR}/rpmbuild/RPMS/#{ARCH}/#{PRODUCT}-#{RELEASE}-#{BLDNUM}.#{ARCH}.rpm", "#{PREFIXD}/#{PRODUCT}_#{PRODUCT_VERSION}_#{ARCH}.rpm"
