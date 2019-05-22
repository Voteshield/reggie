# Veggie

Veggie is a python package for reading state voter registration files.

![veggie_example.gif]

## What is a Voter File?

Voter files, often referred to as voter registration lists, are files kept by state governments to determine which citizens are registered and eligible for elections. Each state keeps a voter file, has different information about each voter (sometimes keeping race, gender, email, phone number, address, and other information), charges a different amount for this public file (Ohio and North Carolina are free, while some states charge more than $10k for a file), and also keeps these files in different formats. These issues make them difficult to read into R/Python/Stata and to analyze. 

## Example Use Case: Florida




## Setup 

$ pip 
- download this repo
```bash
$ git clone git@github.com:Voteshield/Veggie.git 
$ cd Veggie
```
- open the terminal and download pip and virtualenv if you do not have them
```bash
$ sudo easy_install pip
$ pip install vitualenv
```
 - create and start a virtual environment, then install the python requirements
```bash
$ virtualenv venv
$ source venv/bin/activate
(venv)$ pip install -r requirements.txt
```

## Use

## Expected File Format by State