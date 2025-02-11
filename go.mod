module github.com/couchbase/sync_gateway

go 1.23

require (
	dario.cat/mergo v1.0.0
	github.com/KimMachineGun/automemlimit v0.7.0
	github.com/coreos/go-oidc/v3 v3.12.0
	github.com/couchbase/cbgt v1.3.10-0.20250128173458-04138cb9d33d
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/go-blip v0.0.0-20250130142438-e3a29100f703
	github.com/couchbase/gocb/v2 v2.9.4-0.20250206113323-8ef6d9010511
	github.com/couchbase/gocbcore/v10 v10.5.4-0.20250107135314-f4c4becdca29
	github.com/couchbase/gomemcached v0.2.1
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/sg-bucket v0.0.0-20241018143914-45ef51a0c1be
	github.com/couchbaselabs/go-fleecedelta v0.0.0-20220909152808-6d09efa7a338
	github.com/couchbaselabs/gocbconnstr v1.0.5
	github.com/couchbaselabs/rosmar v0.0.0-20240610211258-c856107e8e78
	github.com/elastic/gosigar v0.14.3
	github.com/felixge/fgprof v0.9.5
	github.com/go-jose/go-jose/v4 v4.0.4
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/json-iterator/go v1.1.12
	github.com/kardianos/service v1.2.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common v0.55.0
	github.com/quasilyte/go-ruleguard/dsl v0.3.22
	github.com/robertkrimen/otto v0.0.0-20211024170158-b87d35c0b86f
	github.com/samuel/go-metrics v0.0.0-20150819231912-7ccf3e0e1fb1
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/stretchr/testify v1.10.0
	golang.org/x/crypto v0.32.0
	golang.org/x/exp v0.0.0-20240808152545-0cdaa3abc0fa
	golang.org/x/net v0.34.0
	golang.org/x/oauth2 v0.25.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
)

require (
	github.com/aws/aws-sdk-go v1.44.299 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/websocket v1.8.12 // indirect
	github.com/couchbase/blance v0.1.5 // indirect
	github.com/couchbase/cbauth v0.1.11 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/gocbcoreps v0.1.3 // indirect
	github.com/couchbase/goprotostellar v1.0.2 // indirect
	github.com/couchbase/tools-common/cloud v1.0.0 // indirect
	github.com/couchbase/tools-common/fs v1.0.0 // indirect
	github.com/couchbase/tools-common/testing v1.0.0 // indirect
	github.com/couchbase/tools-common/types v1.0.0 // indirect
	github.com/couchbase/tools-common/utils v1.0.0 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20240607131231-fb385523de28 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/pprof v0.0.0-20240227163752-401108e1b7e7 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/grpc v1.63.2 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
