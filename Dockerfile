ARG BUILDER_IMAGE
FROM ${BUILDER_IMAGE} as builder

WORKDIR /plugin
COPY . .

ARG PLUGIN_NAME
RUN /builder.sh plugins ${PLUGIN_NAME}

FROM alpine:3.16
WORKDIR /plugins

ARG PLUGIN_NAME
COPY --from=builder /plugin/plugins/${PLUGIN_NAME} /plugins/${PLUGIN_NAME}
