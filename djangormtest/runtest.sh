#!/bin/bash

set -e

. ./.env.sh

# generate go models
generate_go_models() {
    # ensure correct pwd
    test -f djangormtest/settings.py

    # remove existing models
    rm -rf models

    DJANGO_SETTINGS_MODULE=djangormtest.settings ../djan-go-rm.py --gomodule djangormtest djangormtestapp
}

# migrate database
migrate_database() {
    python manage.py migrate
}

# populate database
populate_database() {
    python manage.py loaddata djangormtestapp/fixtures/initial_data.json
}

# run tests
run_tests() {
    go run main.go
}

generate_go_models
migrate_database
populate_database
run_tests
