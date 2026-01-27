#!/bin/bash
# Script to convert macOS iconset to Windows .ico format
# This script extracts PNG files from the iconset and combines them into a single .ico file

# Set paths
ICONSET_PATH="../assets/AppSet.iconset"
OUTPUT_DIR="./icon"
OUTPUT_ICO="$OUTPUT_DIR/graph-loom.ico"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if iconset directory exists
if [ ! -d "$ICONSET_PATH" ]; then
    echo "Error: Iconset directory not found at $ICONSET_PATH"
    echo "Please ensure the assets/AppSet.iconset directory exists in your project"
    exit 1
fi

# List available PNG files in iconset
echo "Found PNG files in iconset:"
ls -1 "$ICONSET_PATH"/*.png 2>/dev/null

# Windows .ico format typically includes these sizes:
# 16x16, 32x32, 48x48, 256x256
# We'll look for the closest matches in the iconset

# macOS iconset naming convention:
# icon_16x16.png, icon_16x16@2x.png (32x32)
# icon_32x32.png, icon_32x32@2x.png (64x64)
# icon_128x128.png, icon_128x128@2x.png (256x256)
# icon_256x256.png, icon_256x256@2x.png (512x512)
# icon_512x512.png, icon_512x512@2x.png (1024x1024)

# Find the best available icons
ICON_16=""
ICON_32=""
ICON_48=""
ICON_256=""

# Look for 16x16
if [ -f "$ICONSET_PATH/icon_16x16.png" ]; then
    ICON_16="$ICONSET_PATH/icon_16x16.png"
fi

# Look for 32x32 (could be icon_16x16@2x.png or icon_32x32.png)
if [ -f "$ICONSET_PATH/icon_16x16@2x.png" ]; then
    ICON_32="$ICONSET_PATH/icon_16x16@2x.png"
elif [ -f "$ICONSET_PATH/icon_32x32.png" ]; then
    ICON_32="$ICONSET_PATH/icon_32x32.png"
fi

# Look for 48x48 (might need to resize from 32x32@2x or 128x128)
if [ -f "$ICONSET_PATH/icon_32x32@2x.png" ]; then
    # Resize 64x64 to 48x48
    convert "$ICONSET_PATH/icon_32x32@2x.png" -resize 48x48 "$OUTPUT_DIR/icon_48x48.png"
    ICON_48="$OUTPUT_DIR/icon_48x48.png"
elif [ -f "$ICONSET_PATH/icon_128x128.png" ]; then
    # Resize 128x128 to 48x48
    convert "$ICONSET_PATH/icon_128x128.png" -resize 48x48 "$OUTPUT_DIR/icon_48x48.png"
    ICON_48="$OUTPUT_DIR/icon_48x48.png"
fi

# Look for 256x256 (could be icon_128x128@2x.png or icon_256x256.png)
if [ -f "$ICONSET_PATH/icon_128x128@2x.png" ]; then
    ICON_256="$ICONSET_PATH/icon_128x128@2x.png"
elif [ -f "$ICONSET_PATH/icon_256x256.png" ]; then
    ICON_256="$ICONSET_PATH/icon_256x256.png"
fi

# Build the convert command with available icons
ICON_FILES=()
if [ -n "$ICON_16" ]; then
    ICON_FILES+=("$ICON_16")
fi
if [ -n "$ICON_32" ]; then
    ICON_FILES+=("$ICON_32")
fi
if [ -n "$ICON_48" ]; then
    ICON_FILES+=("$ICON_48")
fi
if [ -n "$ICON_256" ]; then
    ICON_FILES+=("$ICON_256")
fi

# If no specific sizes found, use any PNG we can find
if [ ${#ICON_FILES[@]} -eq 0 ]; then
    echo "No standard icon sizes found, using first available PNG..."
    FIRST_PNG=$(ls "$ICONSET_PATH"/*.png 2>/dev/null | head -1)
    if [ -n "$FIRST_PNG" ]; then
        # Create multiple sizes from this one image
        convert "$FIRST_PNG" -resize 16x16 "$OUTPUT_DIR/icon_16x16.png"
        convert "$FIRST_PNG" -resize 32x32 "$OUTPUT_DIR/icon_32x32.png"
        convert "$FIRST_PNG" -resize 48x48 "$OUTPUT_DIR/icon_48x48.png"
        convert "$FIRST_PNG" -resize 256x256 "$OUTPUT_DIR/icon_256x256.png"
        
        ICON_FILES=("$OUTPUT_DIR/icon_16x16.png" "$OUTPUT_DIR/icon_32x32.png" "$OUTPUT_DIR/icon_48x48.png" "$OUTPUT_DIR/icon_256x256.png")
    else
        echo "Error: No PNG files found in iconset directory"
        exit 1
    fi
fi

# Convert to .ico format
echo "Creating Windows .ico file with ${#ICON_FILES[@]} sizes..."
convert "${ICON_FILES[@]}" "$OUTPUT_ICO"

if [ $? -eq 0 ]; then
    echo "Successfully created: $OUTPUT_ICO"
    echo "Icon file is ready to use with Inno Setup"
else
    echo "Error: Failed to create .ico file"
    exit 1
fi

# Clean up temporary files
if [ -f "$OUTPUT_DIR/icon_48x48.png" ]; then
    rm "$OUTPUT_DIR/icon_48x48.png"
fi
