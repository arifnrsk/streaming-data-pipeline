"""Setup script for the streaming data pipeline package."""

from setuptools import setup, find_packages
import os

# Read README file
def read_readme():
    """Read README.md file."""
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

# Read requirements file
def read_requirements():
    """Read requirements.txt file."""
    req_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(req_path):
        with open(req_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="streaming-data-pipeline",
    version="1.0.0",
    description="Real-time data pipeline for processing purchasing events using Kafka and PySpark",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="Data Engineering Student",
    author_email="student@example.com",
    url="https://github.com/username/streaming-data-pipeline",
    
    packages=find_packages(),
    include_package_data=True,
    
    python_requires=">=3.8",
    install_requires=read_requirements(),
    
    extras_require={
        'dev': [
            'pytest>=7.4.3',
            'black>=23.12.0',
            'flake8>=6.1.0',
            'pytest-cov>=4.1.0',
            'pytest-mock>=3.12.0',
        ],
        'docs': [
            'sphinx>=7.1.0',
            'sphinx-rtd-theme>=1.3.0',
        ]
    },
    
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: Database",
    ],
    
    keywords="streaming kafka pyspark data-pipeline real-time analytics",
    
    entry_points={
        'console_scripts': [
            'streaming-producer=src.producer.event_producer:main',
            'streaming-consumer=src.consumer.streaming_job:main',
        ],
    },
    
    project_urls={
        'Documentation': 'https://github.com/username/streaming-data-pipeline#readme',
        'Source': 'https://github.com/username/streaming-data-pipeline',
        'Tracker': 'https://github.com/username/streaming-data-pipeline/issues',
    },
) 