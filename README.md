# Veggie

Veggie is a python package for reading state voter registration files.


## Command Line Example
![](veggie_example.gif)

## Python Example

![](veggie_python_example.gif)

## What is a Voter File?

Voter files, often referred to as voter registration lists, are files kept by state governments to determine which citizens are registered and eligible for elections. Each state keeps a voter file, has different information about each voter (sometimes keeping race, gender, email, phone number, address, and other information), charges a different amount for this public file (Ohio and North Carolina are free, while some states charge more than $10k for a file), and also keeps these files in different formats. These issues make it frustratingly difficult to get insight into how a state deals with their voters' data, and also laborious to read into analytics software like R/Python/Stata. 

## Veggie

Veggie will take a state voter file and return a compressed csv file with voter history at the row level

## Example Use Case: North Carolina

The North Carolina voter file is used in these examples because it is open to the public


## Installation 

$ pip install 
- download this repo
```bash
$ pip install veggie
```

## Use

## Expected File Format by State