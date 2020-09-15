# Sync Gateway Grafana Dashboard #

**Requirement**: [jsonnet](https://jsonnet.org/)  
**Requirement**: [golang](https://golang.org/)

The provided dashboard.jsonnet is used to generate a Grafana compatible dashboard.json file using jsonnet and [grafonnet-lib](grafonnet-lib)

In order to aid in the process of setting this up a makefile was made which should make the process easier. The two options that the makefile provides are the following:

---
Option 1: 
```
make grafana
```
This command will generate the grafana dashboard.json file which can be imported into Grafana.

---
Option 2: 
```
make grafana-dev
```
This command will run the above command in order to generate the dashboard.json file and will then attempt to upload the dashboard to a Grafana instance which is expected to be running on localhost:3000

---

If one is unable to run the makefile, below is a breakdown of what operations the makefile and scripts perform:

**make grafana**

- Runs `go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb` in order to get the jsonnet-bundler
- Initialize jsonnet bundler directory by running `jb init`
- Utilizes jsonnet bundler in order to install grafonnet `jb install https://github.com/grafana/grafonnet-lib/grafonnet`
- Finally generates the dashboard json using jsonnet and grafonnet using `jsonnet -J grafana dashboard.jsonnet -o ./dashboard.json`

**make grafana-dev**

- Runs the above actions in order to generate the Grafana dashboard.json file
- Then uploads that dashboard.json file to localhost:3000 (requires Grafana instance to be active over that address) - This requires a couple of operations in order to do this which can be found in `install_grafana.sh`

--- 

### Troubleshooting

When running the makefile or when running jb init you may encounter the following error:

`jb: command not found`

If this occurs then your environment was unable to find the `jb` binary which means the GOPATH, or the installation directory is not part of your PATH environment variable.

The GOPATH can be found with `echo $(go env GOPATH)`. This is where `jb` will be installed. Ensure that this directory is present in the PATH variable. 