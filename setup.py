from setuptools import setup, find_packages
from korus.__init__ import __version__

setup(
    name="korus",
    version=__version__,
    description="Python package for managing acoustic metadata and annotations",
    author="Oliver Kirsebom",
    author_email="oliver.kirsebom@gmail.com",
    url=None,
    license="",
    packages=find_packages(),
    install_requires=[
        "soundfile", 
        "pandas", 
        "numpy",
        "termcolor", 
        "treelib", 
        "tqdm"
    ],
    entry_points={
        "console_scripts": [
            "korus-submit = korus.app.submit:main",
        ],
    },
    setup_requires=["pytest-runner", "wheel"],
    tests_require=["pytest",],
    include_package_data=True,
    zip_safe=False,
)
