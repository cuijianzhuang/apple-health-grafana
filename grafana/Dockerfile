FROM grafana/grafana:13.1.1
ENV GF_DEFAULT_LANGUAGE=zh-Hans
ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/dashboards/dashs/specific-metrics.json

COPY provisioning/ /etc/grafana/provisioning/
