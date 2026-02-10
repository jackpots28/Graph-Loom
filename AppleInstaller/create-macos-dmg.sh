#!/bin/bash
# macOS DMG Installer Script
# This script creates a macOS .app bundle and packages it into a DMG installer
#
# Usage: ./create-macos-dmg.sh [options]
#   Options can be set via environment variables or by editing the configuration below.
#
# Configuration can be customized by setting these environment variables before running:
#   APP_NAME          - Application name (default: Graph-Loom)
#   APP_VERSION       - Application version (default: 1.9.2)
#   APP_BUNDLE_ID     - Bundle identifier (default: com.example.app-name)
#   APP_PUBLISHER     - Publisher name (default: Your Name)
#   APP_URL           - Publisher URL (default: https://github.com/yourusername)
#   EXECUTABLE_NAME   - Name of the built executable (default: same as APP_NAME)
#   ICON_FILE         - Icon filename in assets folder (default: AppSet.icns)
#   APP_CATEGORY      - macOS app category (default: public.app-category.developer-tools)
#   MIN_MACOS_VERSION - Minimum macOS version (default: 10.13)
#   COPYRIGHT_YEAR    - Copyright year (default: current year)

set -e

# ============================================================================
# CONFIGURATION - Customize these values for your project
# ============================================================================

# Application metadata (override with environment variables or edit defaults)
APP_NAME="${APP_NAME:-Graph-Loom}"
APP_VERSION="${APP_VERSION:-1.9.2}"
APP_BUNDLE_ID="${APP_BUNDLE_ID:-com.example.$(echo "$APP_NAME" | tr '[:upper:]' '[:lower:]')}"
APP_PUBLISHER="${APP_PUBLISHER:-Your Name}"
APP_URL="${APP_URL:-https://github.com/yourusername}"

# Build settings
EXECUTABLE_NAME="${EXECUTABLE_NAME:-$APP_NAME}"
ICON_FILE="${ICON_FILE:-AppSet.icns}"

# macOS specific settings
APP_CATEGORY="${APP_CATEGORY:-public.app-category.developer-tools}"
MIN_MACOS_VERSION="${MIN_MACOS_VERSION:-10.13}"
COPYRIGHT_YEAR="${COPYRIGHT_YEAR:-$(date +%Y)}"

# ============================================================================
# PATHS - Generally don't need to modify these
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$SCRIPT_DIR/build"
APP_BUNDLE="$BUILD_DIR/$APP_NAME.app"
DMG_OUTPUT="$SCRIPT_DIR/output"
DMG_NAME="$APP_NAME-$APP_VERSION"

# Icon path
ICON_PATH="$PROJECT_ROOT/assets/$ICON_FILE"

# Clean previous builds
echo "=== Cleaning previous builds ==="
rm -rf "$BUILD_DIR"
rm -rf "$DMG_OUTPUT"
mkdir -p "$BUILD_DIR"
mkdir -p "$DMG_OUTPUT"

# Check if cargo-bundle is installed
if ! cargo bundle --help > /dev/null 2>&1; then
    echo "=== Installing cargo-bundle ==="
    cargo install cargo-bundle
fi

# Build the app bundle using cargo bundle
echo "=== Building $APP_NAME using cargo bundle --release ==="
cd "$PROJECT_ROOT"
cargo bundle --release

# Find the generated .app bundle from cargo bundle output
CARGO_BUNDLE_OUTPUT="$PROJECT_ROOT/target/release/bundle/osx"
CARGO_APP_BUNDLE=$(find "$CARGO_BUNDLE_OUTPUT" -maxdepth 1 -name "*.app" -type d 2>/dev/null | head -1)

if [ -z "$CARGO_APP_BUNDLE" ] || [ ! -d "$CARGO_APP_BUNDLE" ]; then
    echo "Error: Could not find .app bundle in $CARGO_BUNDLE_OUTPUT"
    echo "Make sure your Cargo.toml has [package.metadata.bundle] configuration"
    exit 1
fi

echo "Found cargo bundle output: $CARGO_APP_BUNDLE"

# Copy the cargo bundle output to our build directory
cp -R "$CARGO_APP_BUNDLE" "$APP_BUNDLE"

echo "=== App bundle created at $APP_BUNDLE ==="

# Create the DMG
echo "=== Creating DMG installer ==="

# Create a temporary directory for DMG contents
DMG_TEMP="$BUILD_DIR/dmg_temp"
mkdir -p "$DMG_TEMP"

# Copy the app bundle to the temp directory
cp -R "$APP_BUNDLE" "$DMG_TEMP/"

# Create a symbolic link to Applications folder
ln -s /Applications "$DMG_TEMP/Applications"

# Create the DMG
DMG_PATH="$DMG_OUTPUT/$DMG_NAME.dmg"

# Remove existing DMG if present
rm -f "$DMG_PATH"

# Create DMG using hdiutil
echo "Creating DMG..."
hdiutil create -volname "$APP_NAME" \
    -srcfolder "$DMG_TEMP" \
    -ov -format UDZO \
    "$DMG_PATH"

# Clean up
rm -rf "$DMG_TEMP"

echo ""
echo "=== Build Complete ==="
echo "App Bundle: $APP_BUNDLE"
echo "DMG Installer: $DMG_PATH"
echo ""
echo "To install:"
echo "  1. Open the DMG file"
echo "  2. Drag $APP_NAME to the Applications folder"
echo "  3. Eject the DMG"
echo "  4. Launch $APP_NAME from Applications"
