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
    install_requires=["pyyaml", "redis", "hiredis", "python-dotenv", "sqlalchemy", "psycopg2-binary"],
    test_requires=["pytest"],
)