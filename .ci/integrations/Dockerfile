FROM apachepulsar/pulsar:latest

COPY .ci/integrations/test-pulsar-io-google-pubsub-*.yaml /test-pulsar-io-google-pubsub/

COPY target/*.nar /test-pulsar-io-google-pubsub/pulsar-io-google-pubsub.nar

ENTRYPOINT ["/pulsar/bin/pulsar", "standalone"]
