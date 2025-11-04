# Packaging and Distribution Guide

This document explains how to build and distribute the VGP Planemo Scripts package.

## Package Structure

The package is now pip-installable with the following structure:

```
VGP-planemo-scripts/
├── setup.py              # Package configuration
├── requirements.txt      # Python dependencies
├── MANIFEST.in          # Include templates and docs
├── README.md            # Main documentation
├── .gitignore           # Git ignore rules
└── batch_vgp_run/       # Main package
    ├── __init__.py      # Package initialization
    ├── *.py             # Script files
    └── templates/       # YAML templates
        └── *.yaml
```

## Building the Package

### Build source distribution

```bash
python setup.py sdist
```

This creates `dist/vgp_planemo_scripts-1.0.0.tar.gz`

### Build wheel distribution (optional, requires wheel package)

```bash
pip install wheel
python setup.py bdist_wheel
```

## Installing Locally for Testing

### Install in development mode (editable)

```bash
pip install -e .
```

This allows you to edit the code and see changes immediately.

### Install from built package

```bash
pip install dist/vgp_planemo_scripts-1.0.0.tar.gz
```

### Uninstall

```bash
pip uninstall vgp-planemo-scripts
```

## Publishing to PyPI

### Prerequisites

1. Create an account on [PyPI](https://pypi.org/account/register/)
2. Create an account on [TestPyPI](https://test.pypi.org/account/register/) (for testing)
3. Install twine: `pip install twine`

### Test on TestPyPI First

```bash
# Build the package
python setup.py sdist bdist_wheel

# Upload to TestPyPI
twine upload --repository testpypi dist/*

# Test installation from TestPyPI
pip install --index-url https://test.pypi.org/simple/ vgp-planemo-scripts
```

### Publish to PyPI

```bash
# Build the package
python setup.py sdist bdist_wheel

# Upload to PyPI
twine upload dist/*

# Users can now install with:
# pip install vgp-planemo-scripts
```

## Version Management

Update version in three places:
1. `setup.py` - `version` parameter
2. `batch_vgp_run/__init__.py` - `__version__` variable
3. Create a git tag: `git tag -a v1.0.0 -m "Release 1.0.0"`

## Creating a New Release

```bash
# 1. Update version numbers
# Edit setup.py and batch_vgp_run/__init__.py

# 2. Commit changes
git add setup.py batch_vgp_run/__init__.py
git commit -m "Bump version to 1.0.1"

# 3. Create tag
git tag -a v1.0.1 -m "Release 1.0.1"

# 4. Push to GitHub
git push origin main
git push origin v1.0.1

# 5. Build and upload to PyPI
python setup.py sdist bdist_wheel
twine upload dist/*
```

## Entry Points

After installation, these commands become available:

- `vgp-run-all` - Main automated pipeline
- `vgp-get-urls` - Get GenomeArk URLs
- `vgp-download-reports` - Download workflow reports
- `vgp-fetch-invocations` - Fetch invocation numbers
- `vgp-prepare-wf[0,1,3,4,8,9]` - Prepare individual workflows

## Testing the Package

```bash
# Install in development mode
pip install -e .

# Test commands are available
vgp-run-all --help
vgp-get-urls --help

# Run a test
vgp-get-urls -t test_species.tsv
```

## Common Issues

### Templates not included

If templates are missing after installation:
- Check MANIFEST.in includes `recursive-include batch_vgp_run/templates *.yaml`
- Check setup.py has `include_package_data=True`
- Check setup.py has `package_data` entry

### Scripts not in PATH

If commands are not found after installation:
- Check entry_points in setup.py
- Ensure pip install location is in PATH
- Try: `python -m batch_vgp_run.run_all` as alternative

### Import errors

If imports fail:
- Check `__init__.py` exists in batch_vgp_run/
- Check all dependencies in requirements.txt are installed
- Try reinstalling: `pip install --force-reinstall .`
