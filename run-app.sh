#!/bin/bash

set -e

echo "🚀 Starting Rusty Knowledge Tauri MVP..."
echo ""

if [ ! -d "node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    npm install
    echo ""
fi

echo "🏗️  Building and running Tauri application..."
npm run tauri dev
