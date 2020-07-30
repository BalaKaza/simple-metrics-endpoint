#!/usr/bin/env bash

SCRIPT_DIR="$(dirname ${0})"
fly581 -t concourse.gemfire-ci.info status ||
    fly581 -t concourse.gemfire-ci.info login -n developer -c https://concourse.gemfire-ci.info
fly581 -t concourse.gemfire-ci.info set-pipeline -p tanzu-wavefront-integration -c "${SCRIPT_DIR}/pipeline.yml"
