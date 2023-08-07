module github.com/couchbase/sync_gateway

go 1.19

require (
	github.com/bhoriuchi/graphql-go-tools v1.0.0
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/couchbase/cbgt v1.3.4
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-blip v0.0.0-20230510201532-fcadc404bd41
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/gocb/v2 v2.6.2
	github.com/couchbase/gocbcore/v10 v10.2.4-0.20230511103754-8dd1a95f5f33
	github.com/couchbase/gomemcached v0.2.1
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/sg-bucket v0.0.0-20230803171414-d33c89bb2756
	github.com/couchbaselabs/go-fleecedelta v0.0.0-20200408160354-2ed3f45fde8f
	github.com/couchbaselabs/rosmar v0.0.0-20230807001736-0fe617bb9440
	github.com/elastic/gosigar v0.14.2
	github.com/felixge/fgprof v0.9.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/graphql-go/graphql v0.8.0
	github.com/imdario/mergo v0.3.12
	github.com/json-iterator/go v1.1.12
	github.com/kardianos/service v1.2.1
	github.com/natefinch/lumberjack v2.0.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/robertkrimen/otto v0.0.0-20211024170158-b87d35c0b86f
	github.com/samuel/go-metrics v0.0.0-20150819231912-7ccf3e0e1fb1
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/stretchr/testify v1.8.4
	golang.org/x/crypto v0.11.0
	golang.org/x/exp v0.0.0-20230801115018-d63ba01acd4b
	golang.org/x/net v0.13.0
	golang.org/x/oauth2 v0.0.0-20220718184931-c8730f7fcb92
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4
	gopkg.in/square/go-jose.v2 v2.6.0
)

replace github.com/graphql-go/graphql v0.8.0 => github.com/couchbasedeps/graphql-go v0.8.1

require (
	github.com/aws/aws-sdk-go v1.44.77 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/couchbase/blance v0.1.3 // indirect
	github.com/couchbase/cbauth v0.1.10 // indirect
	github.com/couchbase/tools-common v0.0.0-20220810163003-4c3c185822d4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20221118152302-e6195bd50e26 // indirect
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-sqlite3 v1.14.17 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/cachecontrol v0.1.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/couchbase/gocb.v1 v1.6.7 // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	nhooyr.io/websocket v1.8.7 // indirect
)
