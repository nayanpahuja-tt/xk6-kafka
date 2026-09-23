#!/usr/bin/env bash
set -euo pipefail

# Run entire deployment as a single remote bash session to avoid local quoting issues.
ssh ubuntu@172.30.26.14 bash -s << 'REMOTE'
set -euo pipefail

echo "==> Patching vm-operator image to ECR..."
kubectl patch deployment vm-operator -n monitoring \
  --type=strategic \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"manager","image":"726621696910.dkr.ecr.ap-south-1.amazonaws.com/vm-operator:v0.44.0"}]}}}}'

echo "==> Waiting for operator rollout..."
kubectl rollout status deployment/vm-operator -n monitoring --timeout=120s

echo "==> Applying VMAgent CR..."
kubectl apply -f - << 'YAML'
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMAgent
metadata:
  name: prod
  namespace: monitoring
spec:
  selectAllByDefault: true

  image:
    repository: 726621696910.dkr.ecr.ap-south-1.amazonaws.com/vmagent
    tag: v1.136.0
    pullPolicy: IfNotPresent

  remoteWrite:
    - url: http://vminsert-prod-vmcluster.monitoring.svc.cluster.local:8480/insert/0/prometheus/api/v1/write

  remoteWriteSettings:
    tmpDataPath: /tmp/vmagent-remotewrite-data
    maxDiskUsagePerURL: 1073741824

  statefulMode: true
  statefulStorage:
    volumeClaimTemplate:
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: gp3
        resources:
          requests:
            storage: 10Gi

  serviceAccountName: vmagent-victoria-metrics-agent

  inlineScrapeConfig: |
    - job_name: kubernetes-apiservers
      bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      kubernetes_sd_configs:
        - role: endpoints
      relabel_configs:
        - action: keep
          regex: default;kubernetes;https
          source_labels:
            - __meta_kubernetes_namespace
            - __meta_kubernetes_service_name
            - __meta_kubernetes_endpoint_port_name
      scheme: https
      tls_config:
        ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
        insecure_skip_verify: true
    - job_name: kubernetes-nodes
      bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      kubernetes_sd_configs:
        - role: node
      relabel_configs:
        - action: labelmap
          regex: __meta_kubernetes_node_label_(.+)
      scheme: https
      tls_config:
        ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
        insecure_skip_verify: true
    - job_name: kubernetes-nodes-cadvisor
      bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      honor_timestamps: false
      kubernetes_sd_configs:
        - role: node
      metrics_path: /metrics/cadvisor
      relabel_configs:
        - action: labelmap
          regex: __meta_kubernetes_node_label_(.+)
      scheme: https
      tls_config:
        ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
        insecure_skip_verify: true
    - job_name: kubernetes-service-endpoints
      kubernetes_sd_configs:
        - role: endpointslices
      relabel_configs:
        - action: drop
          regex: "true"
          source_labels:
            - __meta_kubernetes_pod_container_init
        - action: keep_if_equal
          source_labels:
            - __meta_kubernetes_service_annotation_prometheus_io_port
            - __meta_kubernetes_pod_container_port_number
        - action: keep
          regex: "true"
          source_labels:
            - __meta_kubernetes_service_annotation_prometheus_io_scrape
        - action: replace
          regex: (https?)
          source_labels:
            - __meta_kubernetes_service_annotation_prometheus_io_scheme
          target_label: __scheme__
        - action: replace
          regex: (.+)
          source_labels:
            - __meta_kubernetes_service_annotation_prometheus_io_path
          target_label: __metrics_path__
        - action: replace
          regex: ([^:]+)(?::\d+)?;(\d+)
          replacement: $1:$2
          source_labels:
            - __address__
            - __meta_kubernetes_service_annotation_prometheus_io_port
          target_label: __address__
        - action: labelmap
          regex: __meta_kubernetes_service_label_(.+)
        - source_labels:
            - __meta_kubernetes_pod_name
          target_label: pod
        - source_labels:
            - __meta_kubernetes_pod_container_name
          target_label: container
        - source_labels:
            - __meta_kubernetes_namespace
          target_label: namespace
        - source_labels:
            - __meta_kubernetes_service_name
          target_label: service
        - replacement: ${1}
          source_labels:
            - __meta_kubernetes_service_name
          target_label: job
        - action: replace
          source_labels:
            - __meta_kubernetes_pod_node_name
          target_label: node
      metric_relabel_configs:
        - action: replace
          regex: (.+)
          source_labels:
            - exported_namespace
          target_label: namespace
        - action: replace
          regex: (.+)
          source_labels:
            - exported_pod
          target_label: pod
        - action: replace
          regex: (.+)
          source_labels:
            - exported_node
          target_label: node
    - job_name: kubernetes-pods
      kubernetes_sd_configs:
        - role: pod
      relabel_configs:
        - action: drop
          regex: "true"
          source_labels:
            - __meta_kubernetes_pod_container_init
        - action: keep_if_equal
          source_labels:
            - __meta_kubernetes_pod_annotation_prometheus_io_port
            - __meta_kubernetes_pod_container_port_number
        - action: keep
          regex: "true"
          source_labels:
            - __meta_kubernetes_pod_annotation_prometheus_io_scrape
        - action: replace
          regex: (.+)
          source_labels:
            - __meta_kubernetes_pod_annotation_prometheus_io_path
          target_label: __metrics_path__
        - action: replace
          regex: ([^:]+)(?::\d+)?;(\d+)
          replacement: $1:$2
          source_labels:
            - __address__
            - __meta_kubernetes_pod_annotation_prometheus_io_port
          target_label: __address__
        - action: labelmap
          regex: __meta_kubernetes_pod_label_(.+)
        - source_labels:
            - __meta_kubernetes_pod_name
          target_label: pod
        - source_labels:
            - __meta_kubernetes_pod_container_name
          target_label: container
        - source_labels:
            - __meta_kubernetes_namespace
          target_label: namespace
        - action: replace
          source_labels:
            - __meta_kubernetes_pod_node_name
          target_label: node

  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

  replicaCount: 1
  port: "8429"
YAML

echo "==> Waiting for operator to create StatefulSet (up to 90s)..."
for i in $(seq 1 18); do
  kubectl get statefulset/vmagent-prod -n monitoring &>/dev/null && break
  printf "    attempt %d/18...\n" "$i"
  sleep 5
done

echo "==> Waiting for vmagent-prod pod to be ready..."
kubectl wait pod \
  -l "app.kubernetes.io/name=vmagent,app.kubernetes.io/instance=prod" \
  -n monitoring \
  --for=condition=Ready \
  --timeout=180s

echo ""
echo "Done. vmagent-prod is running and scraping all VMServiceScrapes."
echo ""
echo "To verify targets (run locally):"
echo "  ssh -L 8429:localhost:8429 ubuntu@172.30.26.14 kubectl port-forward -n monitoring statefulset/vmagent-prod 8429"
echo "  open http://localhost:8429/targets"
echo ""
echo "Once healthy, scale down the old Helm vmagent:"
echo "  kubectl scale deployment vmagent-victoria-metrics-agent -n monitoring --replicas=0"
REMOTE
