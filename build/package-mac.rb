#!/usr/bin/env ruby
# 
####    ./${PKGR} ${PREFIX} ${PREFIXP} ${REVISION} ${GITSPEC} ${PLATFORM}    ${ARCH}
#                                                   master     macosx-x84_64  x86_64
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

PKGNAME="#{PRODUCT}_#{RELEASE}-#{BLDNUM}_#{PLATFORM}"
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
STAGE_DIR = "#{START_DIR}/build/maczip/#{PKGNAME}"
FileUtils.rm_rf   "#{STAGE_DIR}"
FileUtils.mkdir_p "#{STAGE_DIR}"

[["#{START_DIR}", "#{STAGE_DIR}"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        ["manifest.txt.tmpl", "manifest.xml.tmpl"].each do |x|
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
FileUtils.mv  "#{STAGE_DIR}/manifest.txt", "#{PREFIXD}/manifest.txt"
FileUtils.mv  "#{STAGE_DIR}/manifest.xml", "#{PREFIXD}/manifest.xml"

Dir.chdir("#{START_DIR}") do
    sh %{tar --directory #{File.dirname(PREFIXD)} -czf "#{PKGNAME}.tar.gz"   #{File.basename(PREFIXD)}}
    FileUtils.cp                                       "#{PKGNAME}.tar.gz", "#{PREFIXD}/#{PKGNAME}.tar.gz"
end

#FileUtils.cp "#{STAGE_DIR}/rpmbuild/RPMS/#{ARCH}/#{PRODUCT}-#{BLDNUM}-#{RELEASE}.#{ARCH}.rpm", "#{PREFIXD}/#{PRODUCT}_#{PRODUCT_VERSION}_#{ARCH}.rpm"
