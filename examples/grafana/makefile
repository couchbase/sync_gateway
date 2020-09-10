grafana:
	@echo "Generating dashboard dashboard.json"
	@./generate_dashboard.sh
.PHONY: grafana

grafana-dev:
	@make grafana
	@./install_grafana.sh
.PHONY: grafana-dev
