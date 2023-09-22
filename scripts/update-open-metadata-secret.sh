#!/bin/sh

set -e

eval "$(curl -Ssf https://tea.xyz)"
tea +yq

temp_folder=$(python3 -c 'import tempfile; print(tempfile.gettempdir())')
temp_secrets=$temp_folder/dagster-secrets.yaml

kubectl get secret ${SECRET_NAME} -n ${NAMESPACE} -o yaml >$temp_secrets
value=${OPEN_METADATA_TOKEN}
value_base64=$(echo -n "$value" | base64)
yq e ".data.OPEN_METADATA_TOKEN = \"$value_base64\"" -i $temp_secrets
kubectl apply -f $temp_secrets
rm $temp_secrets

# We need to patch the deployment to force the secret to be updated
kubectl rollout restart deployment ${DEPLOYMENT_NAME} -n ${NAMESPACE}
