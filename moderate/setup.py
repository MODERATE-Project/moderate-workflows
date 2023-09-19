from setuptools import find_packages, setup

_VERSION_DAGSTER = "1.4.14"
_VERSION_DAGSTER_EXT = "0.20.14"

setup(
    name="moderate",
    packages=find_packages(exclude=["moderate_tests"]),
    install_requires=[
        f"dagster=={_VERSION_DAGSTER}",
        f"dagster-postgres=={_VERSION_DAGSTER_EXT}",
        f"dagster-k8s=={_VERSION_DAGSTER_EXT}",
        "pandas==2.0.3",
    ],
    extras_require={
        "dev": [
            f"dagster-webserver=={_VERSION_DAGSTER}",
            "pytest==7.4.2",
            "black==23.9.1",
        ]
    },
)
