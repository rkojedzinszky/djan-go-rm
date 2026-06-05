#!/bin/sh

# Require bash for correct operation
if [ -z "$BASH_VERSION" ]; then
    echo "ERROR: .env.sh requires bash to run. Please use 'bash' or 'source' with bash."
    return 1
fi

# Check if script is being sourced (not executed directly)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "WARNING: .env.sh should be sourced, not executed directly."
    echo "Please run: source .env.sh"
    exit 1
fi

# Get the directory of this script regardless of where it's sourced from
ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -a
source "${ENV_DIR}/.env"
set +a
