import versioneer
from setuptools import find_packages, setup

setup(
    name="redsql",
    packages=find_packages(exclude=["test", "test.*"]),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Generic Redis to SQL data synchronizer.",
    author="AIT Austrian Institute of Technology",
    license="Proprietary",
    url="https://gitlab-intern.ait.ac.at/ees/rdp/generic-components/redsql",
    setup_requires=["pytest-runner"],
    install_requires=[
        "pyyaml>=6.0",
        "redis>=4.3",
        "python-dotenv>=0.20",
        "sqlalchemy>=1.4"
    ],
    test_requires=["pytest"],
)