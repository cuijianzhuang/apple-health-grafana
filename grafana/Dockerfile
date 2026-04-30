FROM grafana/grafana:13.0
ENV GF_USERS_DEFAULT_LANGUAGE=zh-Hans
ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/dashboards/dashs/health-auto-export-overview.json

COPY provisioning/ /etc/grafana/provisioning/
