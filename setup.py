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
    install_requires=["pandas",
                      "click",
                      "numpy",
                      "zipfile2",
                      "PyYAML",
                      "boto3",
                      "requests",
                      "xlrd",
                      "bs4"],
    url="https://github.com/Voteshield/reggie",
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points="""
    [console_scripts]
    reg=reggie:convert_cli
    """,
)
