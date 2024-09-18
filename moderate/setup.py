from setuptools import find_packages, setup

_VERSION_DAGSTER = "1.6.0"
_VERSION_DAGSTER_EXT = "0.22.0"

setup(
    name="moderate",
    version="0.1.0",
    packages=find_packages(exclude=["moderate_tests"]),
    install_requires=[
        f"dagster=={_VERSION_DAGSTER}",
        f"dagster-postgres=={_VERSION_DAGSTER_EXT}",
        f"dagster-k8s=={_VERSION_DAGSTER_EXT}",
        "pandas[performance,postgresql]>=2.0,<3.0",
        "numpy>=1.0,<2.0",
        "python-keycloak>=2.16.1,<3.0",
        "sh==2.0.6",
        "openmetadata-ingestion[postgres,datalake-s3,datalake-gcp]==1.3.1",
        "google-cloud-storage>=2.10,<3.0",
        "sqllineage>=1.4,<1.5",
        "SQLAlchemy>=1.4,<1.5",
        "PyYAML==6.0.1",
        "urllib3<2.0",
        "trino>=0.321.0,<=0.327",
        "requests>=2.0,<3.0",
        "python-slugify>=8.0,<9.0",
        "boto3>=1.29,<2.0",
    ],
    extras_require={
        "dev": [
            f"dagster-webserver=={_VERSION_DAGSTER}",
            "pytest",
            "black",
        ]
    },
)
