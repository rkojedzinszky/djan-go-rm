#!/bin/sh

# This generates Go models

set -e

# ensure correct pwd
test -f djangormtest/settings.py

# remove existing models
rm -rf models

DJANGO_SETTINGS_MODULE=djangormtest.settings ../djan-go-rm.py --gomodule djangormtest djangormtestapp
