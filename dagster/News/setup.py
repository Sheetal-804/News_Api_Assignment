from setuptools import find_packages, setup

setup(
    name="News",
    packages=find_packages(exclude=["News_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
