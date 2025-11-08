#!/bin/bash

# VGP Planemo Scripts - Dependency Installation
# This script installs all required dependencies for the VGP pipeline
#
# SLURM/HPC USERS: If you're on a SLURM cluster:
# 1. Make sure ~/.local/bin has execute permissions (check: mount | grep home)
# 2. If your home directory is on a noexec filesystem, install to a different location
# 3. Add export PATH="$HOME/.local/bin:$PATH" to your ~/.bashrc
# 4. Source your bashrc in SLURM job scripts: source ~/.bashrc

set -e  # Exit on error

echo "Installing VGP Planemo Scripts dependencies..."

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install VGP Planemo Scripts package (creates command-line tools)
echo ""
echo "Installing VGP Planemo Scripts package..."
pip install -e .

# Install NCBI datasets command-line tool
echo ""
echo "Installing NCBI datasets command-line tool..."

# Check if datasets is already installed
if command -v datasets > /dev/null 2>&1; then
    echo "✓ NCBI datasets tool is already installed"
    datasets --version
    SKIP_DATASETS_INSTALL=true
else
    SKIP_DATASETS_INSTALL=false
fi

if [ "$SKIP_DATASETS_INSTALL" = "false" ]; then
    # Detect platform using uname (more portable than $OSTYPE)
    PLATFORM=$(uname -s)
    case "$PLATFORM" in
        Darwin*)
            # macOS
            DATASETS_URL="https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/mac/datasets"
            echo "Detected macOS platform"
            ;;
        Linux*)
            # Linux
            DATASETS_URL="https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets"
            echo "Detected Linux platform"
            ;;
        *)
            echo "Warning: Unsupported platform '$PLATFORM'. Please install NCBI datasets manually from:"
            echo "https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/"
            exit 1
            ;;
    esac

    # Download datasets binary
    # Allow custom install directory (useful for SLURM/HPC with noexec home directories)
    INSTALL_DIR="${DATASETS_INSTALL_DIR:-$HOME/.local/bin}"
    mkdir -p "$INSTALL_DIR"

    echo "Downloading NCBI datasets tool from $DATASETS_URL..."
    echo "Installing to: $INSTALL_DIR"
    curl -o "$INSTALL_DIR/datasets" "$DATASETS_URL"
    chmod +x "$INSTALL_DIR/datasets"

    # Check if the directory allows execution
    if ! "$INSTALL_DIR/datasets" --version > /dev/null 2>&1; then
        echo ""
        echo "WARNING: Could not execute datasets from $INSTALL_DIR"
        echo "This might be because the filesystem is mounted with 'noexec' flag."
        echo "To check: mount | grep \$(df $INSTALL_DIR | tail -1 | awk '{print \$1}')"
        echo ""
        echo "To install to a different location, run:"
        echo "  DATASETS_INSTALL_DIR=/alternative/path bash installs.sh"
        echo "Then add that path to your PATH variable."
    fi

    # Check if install dir is in PATH (using grep for POSIX compatibility)
    if ! echo ":$PATH:" | grep -q ":$INSTALL_DIR:"; then
        echo ""
        echo "Warning: $INSTALL_DIR is not in your PATH"
        echo "Add the following line to your ~/.bashrc or ~/.zshrc:"
        echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
        echo ""
        echo "Then run: source ~/.bashrc  (or source ~/.zshrc)"
    fi
fi

# Verify installation
echo ""
echo "Verifying installations..."

# Check command-line tools
if command -v vgp-run-all > /dev/null 2>&1; then
    echo "✓ VGP command-line tools installed successfully"
    echo "  Available commands: vgp-run-all, vgp-download-reports, vgp-prepare-single"
else
    echo "✗ VGP command-line tools not found"
    echo "  Try running: pip install -e ."
fi

if command -v datasets > /dev/null 2>&1; then
    echo "✓ NCBI datasets tool installed successfully"
    datasets --version
else
    echo "✗ NCBI datasets tool not found in PATH"
    echo "  Please ensure datasets is installed and in your PATH"
fi

if command -v aws > /dev/null 2>&1; then
    echo "✓ AWS CLI installed successfully"
else
    echo "✗ AWS CLI not found"
fi

if command -v planemo > /dev/null 2>&1; then
    echo "✓ Planemo installed successfully"
else
    echo "✗ Planemo not found"
fi

echo ""
echo "============================================================"
echo "Installation complete!"
echo "============================================================"
if command -v vgp-run-all > /dev/null 2>&1; then
    echo ""
    echo "You can now use VGP command-line tools:"
    echo "  vgp-run-all           - Run all VGP workflows automatically"
    echo "  vgp-prepare-single    - Prepare individual workflows (or fetch URLs with --fetch-urls)"
    echo "  vgp-download-reports  - Download workflow reports"
    echo ""
    echo "Example usage:"
    echo "  # Fetch GenomeArk URLs and prepare WF1"
    echo "  vgp-prepare-single --fetch-urls -t species_list.tsv"
    echo "  vgp-prepare-single --workflow 1 -t tracking_runs_species_list.tsv -p profile.yaml"
    echo ""
    echo "  # Or use automated pipeline with URL fetching"
    echo "  vgp-run-all --fetch-urls -t species_list.tsv -p profile.yaml -m ./metadata --id"
fi
echo ""
echo "If you see any ✗ marks above, please check your PATH settings."
echo "For more information, see: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/" 