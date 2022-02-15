module github.com/cbbruno/sync_gateway_mod

go 1.17

require (
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/couchbase/cbgt v1.0.0
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-blip v0.0.0-20210630170037-83b37ec57c97
	github.com/couchbase/go-couchbase v0.1.0
	github.com/couchbase/gocb/v2 v2.3.5
	github.com/couchbase/gocbcore/v10 v10.0.6
	github.com/couchbase/gomemcached v0.1.4
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/sg-bucket v0.0.0-20211203000713-178bb7ca1abb
	github.com/couchbase/sync_gateway v0.0.0-20220110182909-779f11a7c919
	github.com/couchbaselabs/go-fleecedelta v0.0.0-20200408160354-2ed3f45fde8f
	github.com/couchbaselabs/go.assert v0.0.0-20130325201400-cfb33e3a0dac
	github.com/couchbaselabs/walrus v0.0.0-20211203000748-fc018ef7de83
	github.com/elastic/gosigar v0.14.2
	github.com/felixge/fgprof v0.9.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/imdario/mergo v0.3.12
	github.com/json-iterator/go v1.1.12
	github.com/kardianos/service v1.2.1
	github.com/natefinch/lumberjack v2.0.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/robertkrimen/otto v0.0.0-20211024170158-b87d35c0b86f
	github.com/samuel/go-metrics v0.0.0-20150819231912-7ccf3e0e1fb1
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/stretchr/testify v1.7.0
	github.com/tleyden/fakehttp v0.0.0-20150307184655-084795c8f01f
	golang.org/x/crypto v0.0.0-20211215153901-e495a2d5b3d3
	golang.org/x/net v0.0.0-20220107192237-5cfca573fb4d
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	gopkg.in/couchbase/gocb.v1 v1.6.7
	gopkg.in/couchbase/gocbcore.v7 v7.1.18
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4
	gopkg.in/square/go-jose.v2 v2.6.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/couchbase/blance v0.1.1 // indirect
	github.com/couchbase/cbauth v0.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/pprof v0.0.0-20211214055906-6f57359322fd // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/cachecontrol v0.1.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/couchbase/sync_gateway => ./
