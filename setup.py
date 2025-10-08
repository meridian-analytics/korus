from setuptools import setup, find_packages

# get project version
data = dict()
with open("korus/__init__.py") as f:
    exec(f.read(), data)

setup(
    name="korus",
    version=data["__version__"],
    description="Python package for managing acoustic metadata and annotations",
    author="Oliver Kirsebom",
    author_email="oliver.kirsebom@gmail.com",
    url="https://github.com/meridian-analytics/korus",
    license="",
    packages=find_packages(),
    install_requires=[
        "soundfile",
        "pandas",
        "numpy",
        "termcolor",
        "treelib",
        "tqdm",
        "tabulate",
        "pyyaml",
        "inquirer",
        "python-dateutil",
        "datetime_glob",
    ],
    entry_points={
        "console_scripts": [
            "korus-cli = korus.cli.cli:main",
        ],
    },
    setup_requires=["pytest-runner", "build"],
    tests_require=[
        "pytest",
    ],
    include_package_data=True,
    zip_safe=False,
)
