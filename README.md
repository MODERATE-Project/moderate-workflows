# MODERATE workflows

A project that implements the data pipelines for the MODERATE project. These pipelines are built on top of Dagster, which acts as the workflow orchestration service. The pipelines are run on MODERATE's Kubernetes cluster using the Dagster Kubernetes integration.

## Development

### Configure connection to API and S3

Some pipelines depend on the MODERATE's platform API and an S3-compatible object storage service. These dependencies can be configured in a local `.env` file—which will not be committed to the repository—using the following variables:

```console
API_BASE_URL=http://localhost:9080
API_USERNAME=andres.garcia
API_PASSWORD=TheApiPassword
S3_ACCESS_KEY_ID=TheAccessKeyId
S3_SECRET_ACCESS_KEY=TheSecretAccessKey
S3_REGION=europe-west1
S3_BUCKET_NAME=TheBucketName
S3_ENDPOINT_URL=https://storage.googleapis.com
```

These should be optional, so you can still run the pipelines that don't depend on these services.

### Deploy a local Kubernetes-based instance

There's a task in the Taskfile called `start-dev-k8s` that deploys a local Kubernetes cluster using Minikube. This aims to represent, as faithfully as possible, the same environment as the production Kubernetes cluster. It is useful for testing the Dagster integration with Kubernetes locally.

Running `task start-dev-k8s` will do the following:

* Build and push a Docker image containing the code deployment.
* Start the Compose stack with the service dependencies (e.g. Postgres, Keycloak).
* Start Minikube.
* Install Helm charts for both Dagster and OpenMetadata.

Once the task has completed, you can access the OpenMetadata UI and Dagster UI by running `task forward-k8s-open-metadata-ui` and `task forward-k8s-dagster-ui` respectively.

> Due to the particularities of the OpenMetadata OIDC integration with Keycloak, you should access the UIs using your local IP address instead of `localhost`.

#### Create the admin user

Since OpenMetadata is configured to use Keycloak as the OIDC provider, you need to create an admin user in Keycloak in order to be able to log in to OpenMetadata. This can be done in the **Users** section of the Keycloak UI, under the **moderate** realm.

The admin's email address *local-part* must be one of the values included under `authorizer.initialAdmins` in the OpenMetadata YAML values file. Additionally, the *domain* must match the value specified in `authorizer.principalDomain` within the same file:

```console
<value-of-initialAdmins>@<principalDomain>
```

#### Update OpenMetadata token

There's a manual step that needs to be performed in order for the Dagster - OpenMetadata integration to work.

Go to **Settings ▶︎ Bots** in the OpenMetadata UI and click in **ingestion-bot**. Revoke the token and create a new one. Copy the new token and run the following task:

```console
$ OPEN_METADATA_TOKEN="eyJraWQiOiI1NzU2ZDA1Ny0xMTRlL [...]" task update-k8s-open-metadata-token

[...]

+ kubectl wait --for=condition=Ready pods --all --timeout=600s
pod/dagster-daemon-548757dff-zd6hs condition met
pod/dagster-dagster-user-deployments-k8s-code-location-moderat46gft condition met
pod/dagster-dagster-user-deployments-k8s-code-location-moderat68jpc condition met
pod/dagster-dagster-webserver-6bbd5b5c88-5lwzb condition met
pod/openmetadata-5d5dd566d4-ljznx condition met
```

Now go to the Dagster UI and you should be able to successfully materialize the _assets_ that run the OpenMetadata workflows.