buildit:
	./build.sh
clean:
	rm -rf bin pkg
buildclean: clean buildit
cleanbuild: clean buildit
test:
	@./test.sh

grafana:
	@echo "Generating dashboard at ./grafana/dashboard.json"
	@(cd ./examples/grafana && ./generate_dashboard.sh)
.PHONY: grafana

grafana-dev:
	@make grafana
	@(cd ./examples/grafana && ./install_grafana.sh)
.PHONY: grafana-dev
