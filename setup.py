import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='reggie',
    version='0.1',
    author="Tyler Richards",
    author_email="tylerjrichards@gmail.com",
    description="A package for reading in Voter files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=["pandas",
                      "click",
                      "numpy",
                      "zipfile2",
                      "PyYAML<4.3,>=3.10",
                      "boto3",
                      "requests<2.21,>=2.20.0",
                      "xlrd",
                      "bs4"],
    url="https://github.com/Voteshield/reggie",
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points='''
    [console_scripts]
    reg=reggie:convert_cli
    '''
)
