#!/usr/bin/env python3

from setuptools import setup, find_packages
import os

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="vgp-planemo-scripts",
    version="1.0.0",
    author="VGP Team",
    author_email="",
    description="Automated VGP assembly pipeline tools for running workflows through planemo",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Delphine-L/VGP-planemo-scripts",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            # Main automated pipeline
            "vgp-run-all=scripts.run_all:main",

            # Utility scripts
            "vgp-download-reports=scripts.download_reports:main",

            # Unified workflow preparation (includes URL fetching with --fetch-urls)
            "vgp-prepare-single=scripts.prepare_single:main",
        ],
    },
    package_data={
        "batch_vgp_run": ["templates/*.yaml"],
    },
    include_package_data=True,
    keywords="genomics assembly vgp vertebrate-genomes-project planemo galaxy workflow",
    project_urls={
        "Bug Reports": "https://github.com/Delphine-L/VGP-planemo-scripts/issues",
        "Source": "https://github.com/Delphine-L/VGP-planemo-scripts",
    },
)
