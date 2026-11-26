# Spark App Charm

This charm runs small Spark jobs for testing and demos.

## Run the dummy job on Spark 4.0

The charm defaults to Spark 3.4. To opt in to Spark 4.0 for the `dummy`
flavour, override **both** `spark-image` settings:

- The OCI resource supplies the workload container that runs `spark-submit`.
- The charm configuration selects the image for Spark's Kubernetes job pods.

In a Juju Kubernetes model, deploy the charm with both image overrides:

```bash
juju deploy spark-test-app --channel latest/edge --base ubuntu@22.04 \
  --resource spark-image=ghcr.io/canonical/charmed-spark-kyuubi:4.0-22.04_stable \
  --config spark-image=ghcr.io/canonical/charmed-spark:4.0-22.04_stable
```

The charm defaults to the `dummy` flavour and `client` deploy mode. To run the
job, grant cluster permissions and set `NAMESPACE` to the model's Kubernetes
namespace (normally the model name). Deploy Spark Integration Hub if it isn't
already available:

```bash
NAMESPACE=my-juju-model
juju trust spark-test-app --scope=cluster
juju config spark-test-app namespace="$NAMESPACE"
juju deploy spark-integration-hub-k8s --channel 3/stable --trust
juju integrate spark-test-app:spark-service-account \
  spark-integration-hub-k8s:spark-service-account
```

Once `spark-test-app` is active, start the job and check its Pebble logs:

```bash
juju run spark-test-app/0 start-process
juju ssh --container spark spark-test-app/0 /charm/bin/pebble logs -n all spark-job
```

Look for `Total count: 100` and `Running Spark version 4.0.x`. No Kafka or object
storage is needed for this job in client mode. These instructions do not cover the
`kafka` flavour, which still uses Spark 3.4 Kafka connector packages.

### Run the integration test

From the repository root, with Charmcraft and tox installed and the current
Juju controller bootstrapped on Kubernetes, run:

```bash
charmcraft pack
tox run -e integration
```

The test finds the packed charm for the host architecture in the repository
root. Jubilant deploys it and Integration Hub in a temporary model and checks
the job logs for the expected count and Spark version.

Pass `-- --keep-models` to inspect the temporary model after the run. To use
an existing empty Kubernetes model instead, pass `-- --model <model-name>`.
The test does not destroy a model you supply.