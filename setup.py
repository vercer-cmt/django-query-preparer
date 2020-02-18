import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="django-query-preparer",
    version="1.0.1",
    author="Dan Greenhalgh",
    author_email="dgreenhalgh@vercer.co.uk",
    description="A package to prepare queries in postgres before execution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vercer-cmt/django-query-preparer",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: Django",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
