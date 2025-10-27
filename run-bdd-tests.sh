#!/bin/bash

echo "Running Cucumber BDD Tests..."
echo "=============================="
echo ""

cargo test --test cucumber

echo ""
echo "=============================="
echo "Tests complete!"
