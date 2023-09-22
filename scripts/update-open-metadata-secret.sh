#!/bin/sh

set -e

eval "$(curl -Ssf https://tea.xyz)"
tea +yq

temp_folder=$(python3 -c 'import tempfile; print(tempfile.gettempdir())')
temp_secrets=$temp_folder/dagster-secrets.yaml
temp_token=$temp_folder/om-token

kubectl get secret ${SECRET_NAME} -n ${NAMESPACE} -o yaml >$temp_secrets
printf '%s' "$OPEN_METADATA_TOKEN" >$temp_token
value_base64=$(base64 -i $temp_token)
yq e ".data.OPEN_METADATA_TOKEN = \"$value_base64\"" -i $temp_secrets
kubectl apply -f $temp_secrets
rm $temp_secrets
rm $temp_token

# We need to patch the deployment to force the secret to be updated
helm upgrade ${DAGSTER_RELEASE} ${DAGSTER_CHART} --recreate-pods
