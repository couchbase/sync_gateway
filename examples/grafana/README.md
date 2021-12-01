# Sync Gateway Grafana Dashboard #

The provided dashboard.json is Grafana compatible out-of-the-box and can be imported into a Grafana environment directly. 

In order to aid in the process of setting this up a makefile was made which should make the process easier.

---
Dev setup:
```
make grafana-dev
```
The above command will attempt to upload the dashboard.json file to a Grafana instance which is expected to be running on localhost:3000

---

If one is unable to run the makefile, the steps can be performed manually. `make grafana-dev` simply uploads the dashboard.json file to localhost:3000 (requires Grafana instance to be active over that address) - This requires a couple of operations to be performed which can be found in `install_grafana.sh`
