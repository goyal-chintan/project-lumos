"""Setup configuration for Lumos Framework."""
from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Read requirements
requirements = []
with open("requirements.txt") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

# Read dev requirements
dev_requirements = []
try:
    with open("requirements-dev.txt") as f:
        dev_requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]
except FileNotFoundError:
    dev_requirements = [
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0",
        "black>=22.0.0",
        "isort>=5.11.0",
        "mypy>=1.0.0",
        "flake8>=6.0.0",
    ]

setup(
    name="lumos-framework",
    version="0.1.0",
    author="Project Lumos Contributors",
    author_email="",  # Add your email
    description="A pluggable framework for data cataloging, lineage tracking, and metadata management",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/lumos-framework",  # Update with actual URL
    packages=find_packages(exclude=["tests", "tests.*", "orchestration_examples"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    entry_points={
        "console_scripts": [
            "lumos=framework_cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="data-catalog metadata-management lineage data-governance datahub",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/lumos-framework/issues",
        "Source": "https://github.com/yourusername/lumos-framework",
        "Documentation": "https://github.com/yourusername/lumos-framework#readme",
    },
)

