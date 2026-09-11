FROM opensearchproject/opensearch:2.19.0

COPY --chown=opensearch:opensearch build/distributions/datastream-allocator-1.0.0.zip /tmp/
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch \
    file:///tmp/datastream-allocator-1.0.0.zip && \
    rm /tmp/datastream-allocator-1.0.0.zip

RUN echo "cluster.routing.allocation.type: datastream_balanced" >> \
    /usr/share/opensearch/config/opensearch.yml
