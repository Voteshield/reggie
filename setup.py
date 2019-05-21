import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='veggie',
    version='0.1',
    author="Tyler Richards",
    author_email="tylerjrichards@gmail.com",
    description="A package for reading in Voter files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=["pandas", "click", "numpy", "zipfile2", "pyyaml"],
    url="https://github.com/Voteshield/Veggie",
    packages=setuptools.find_packages(),
    include_package_data=True,
    entry_points='''
    [console_scripts]
    veg=veggie:convert_cli
    '''
)
