#!/usr/bin/env ruby
# 
####    ./${PKGR} ${PREFIX} ${PREFIXP} ${REVISION} ${GITSPEC} ${PLATFORM} ${ARCH}

require 'rubygems'
require 'fileutils'
require 'rake'

PRODUCT         = "couchbase-sync-gateway"
PRODUCT_BASE    = "couchbase"
DEBEMAIL        = "build@couchbase.com"

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

sh %{echo "PLATFORM is #{PLATFORM}"}

PKGNAME="#{PRODUCT}_#{RELEASE}"
product_base_cap = PRODUCT_BASE[0..0].upcase + PRODUCT_BASE[1..-1] # Ex: "Couchbase".

STARTDIR  = Dir.getwd()
STAGE_DIR = "#{STARTDIR}/build/deb/#{PKGNAME}-#{BLDNUM}"
FileUtils.rm_rf   "#{STAGE_DIR}"
FileUtils.mkdir_p "#{STAGE_DIR}"
FileUtils.mkdir_p "#{STAGE_DIR}/debian"
FileUtils.mkdir_p "#{STAGE_DIR}/etc"
FileUtils.mkdir_p "#{STAGE_DIR}/opt"

Dir.chdir STAGE_DIR do
# sh %{dh_make -e #{DEBEMAIL} --native --single --packagename #{PKGNAME}}
end

[["#{STARTDIR}", "#{STAGE_DIR}/debian"]].each do |src_dst|
    Dir.chdir(src_dst[0]) do
        Dir.glob("*.tmpl").each do |x|
            target = "#{src_dst[1]}/#{x.gsub('.tmpl', '')}"
            sh %{sed -e s,@@VERSION@@,#{PRODUCT_VERSION},g           #{x} |
                 sed -e s,@@PLATFORM@@,#{PLATFORM},g                      |
                 sed -e s,@@RELEASE@@,#{RELEASE},g                        |
                 sed -e s,@@PREFIX@@,#{PREFIX},g                          |
                 sed -e s,@@REPO_SHA@@,#{REPO_SHA},g                      |
                 sed -e s,@@PRODUCT@@,#{PRODUCT},g                        |
                 sed -e s,@@PRODUCT_EXEC@@,#{PRODUCT_EXEC},g              |
                 sed -e s,@@PRODUCT_BASE@@,#{PRODUCT_BASE},g              |
                 sed -e s,@@PRODUCT_BASE_CAP@@,#{product_base_cap},g      |
                 sed -e s,@@PRODUCT_KIND@@,#{PRODUCT_KIND},g > #{target}}
            sh %{chmod a+x #{target}}
        end 
    end
end
sh %{cp -R "#{PREFIXD}" "#{STAGE_DIR}/opt"}

FileUtils.mv  "#{STAGE_DIR}/debian/manifest.txt", "#{STAGE_DIR}/opt/#{PRODUCT}/manifest.txt"

Dir.chdir STAGE_DIR do
  sh %{dch -b -v "#{PRODUCT_VERSION}" "Released debian package for version #{PRODUCT_VERSION}"}
  sh %{dpkg-buildpackage -b -uc}
end

FileUtils.cp "#{STARTDIR}/build/deb/#{PRODUCT}_#{PRODUCT_VERSION}_#{ARCH}.deb", "#{PREFIXD}"
