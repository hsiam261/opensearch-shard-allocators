ARG OPENSEARCH_VERSION=2.19.0
FROM opensearchproject/opensearch:${OPENSEARCH_VERSION}

ARG OPENSEARCH_VERSION=2.19.0
COPY --chown=opensearch:opensearch build/distributions/datastream-allocator-1.0.0-opensearch-${OPENSEARCH_VERSION}.zip /tmp/
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch \
    file:///tmp/datastream-allocator-1.0.0-opensearch-${OPENSEARCH_VERSION}.zip && \
    rm /tmp/datastream-allocator-1.0.0-opensearch-${OPENSEARCH_VERSION}.zip

RUN echo "cluster.routing.allocation.type: datastream_balanced" >> \
    /usr/share/opensearch/config/opensearch.yml
