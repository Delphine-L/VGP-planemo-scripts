#!/bin/bash

# VGP Planemo Scripts - Dependency Installation
# This script installs all required dependencies for the VGP pipeline

set -e  # Exit on error

echo "Installing VGP Planemo Scripts dependencies..."

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install NCBI datasets command-line tool
echo ""
echo "Installing NCBI datasets command-line tool..."

# Detect platform
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    DATASETS_URL="https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/mac/datasets"
    echo "Detected macOS platform"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    DATASETS_URL="https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets"
    echo "Detected Linux platform"
else
    echo "Warning: Unsupported platform '$OSTYPE'. Please install NCBI datasets manually from:"
    echo "https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/"
    exit 1
fi

# Download datasets binary
INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"

echo "Downloading NCBI datasets tool from $DATASETS_URL..."
curl -o "$INSTALL_DIR/datasets" "$DATASETS_URL"
chmod +x "$INSTALL_DIR/datasets"

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo ""
    echo "Warning: $INSTALL_DIR is not in your PATH"
    echo "Add the following line to your ~/.bashrc or ~/.zshrc:"
    echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "Then run: source ~/.bashrc  (or source ~/.zshrc)"
fi

# Verify installation
echo ""
echo "Verifying installations..."
if command -v datasets &> /dev/null; then
    echo "✓ NCBI datasets tool installed successfully"
    datasets --version
else
    echo "✗ NCBI datasets tool not found in PATH. Please add $INSTALL_DIR to your PATH"
fi

if command -v aws &> /dev/null; then
    echo "✓ AWS CLI installed successfully"
else
    echo "✗ AWS CLI not found"
fi

if command -v planemo &> /dev/null; then
    echo "✓ Planemo installed successfully"
else
    echo "✗ Planemo not found"
fi

echo ""
echo "Installation complete! If you see any ✗ marks above, please check your PATH settings."
echo "For more information, see: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/" 