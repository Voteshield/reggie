import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="reggie",
    version="0.1.0",
    author="VoteShield, Tyler Richards, and contributors",
    author_email="support@voteshield.us",
    description="A package for reading in Voter files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "pandas>=0.23.1",
        "click>=7.1.2",
        "numpy>=1.19.2",
        "zipfile2>=0.0.12",
        "PyYAML>=5.3.1",
        "boto3==1.15.12",
        "requests<2.21,>=2.20.0",
        "xlrd>=1.2.0",
        "bs4>=0.0.1",
        "pytest>=6.0.0",
        "detect_delimiter>=0.1.1",
    ],
    url="https://github.com/Voteshield/reggie",
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points="""
    [console_scripts]
    reg=reggie:convert_cli
    """,
)
