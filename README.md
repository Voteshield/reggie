# Reggie

reggie is a python package for reading state voter registration files created by Voteshield, a data analytics tool designed to protect the integrity of American elections by monitoring changes to the voter rolls.

![](reggie_example.gif)

## Using reggie

reggie will convert voter files into a single compressed voter file along with voter history metadata using either the command line or from within a python environment.  

The current available states are Arizona, Colorado, Florida, Georgia, Iowa, Kansas, Michigan, Minnesota, Missouri, Nevada, New Jersey, New York, North Carolina, Ohio, Pennsylvania, and Texas. Because the voter file for North Carolina is entirely public and can be found on the [state's website](https://dl.ncsbe.gov/index.html?prefix=data/), a smaller version of the NC file is included in the test_data folder. 

## Examples

### Python
```python
from reggie import convert_voter_file

nc_dataframe, metadata = convert_voter_file(state='north_carolina',
                                            local_file='nc_2018-12-22.zip',
                                            date='2018-12-22')
```
### Command Line
```bash
$ reg --state north_carolina --local_file nc_2018-12-22.zip --date 2018-12-22

```

Warning: as voter files are quite large, reggie may take significant time and memory. Internally, Voteshield uses AWS instead of performing these jobs locally. Another option is to use [Colab](https://colab.research.google.com/) to perform larger jobs. 


## Installation 

```bash
$ pip install reggie
```


## What is a Voter File?

Voter files, often referred to as voter registration lists, are files kept by state governments to determine which citizens are registered and eligible for elections. Each state keeps a voter file, has different information about each voter (sometimes keeping race, gender, email, phone number, address, and other information), charges a different amount for this public file (Ohio and North Carolina are free, while some states charge more than $10k for a file), and also keeps these files in different formats. These issues make it frustratingly difficult to get insight into how a state deals with their voters' data, and also laborious to read into analytics software like R/Python/Stata. 

## Voter History

Many states keep voter history in their voter file, often kept in a separate file at the county level. Reggie will combine each of these files, and group the history by turning the collection of voter history into a column in the voter registration file called all_history. The specifics of how this should be done can be [here](https://github.com/Voteshield/reggie/wiki/UVFF-and-State-Onboarding)

## Expected File Format by State

Reggie automatically decompresses all of the files in the provided file, and expects voter files in the format that the individual states use. A small sample of some state's expected input format is below. 

#### Arizona

```
├── MARICOPA\ -\ ACTIVE\ DEM.xlsx
├── MARICOPA\ -\ ACTIVE\ OTHER.xlsx
├── MARICOPA\ -\ ACTIVE\ REP.xlsx
├── MARICOPA\ -\ CANCELLED.xlsx
├── MARICOPA\ -\ INACTIVE.xlsx
├── MARICOPA\ -\ SUSPENSE.xlsx
├── PIMA\ -\ ACTIVE.xlsx
├── PIMA\ -\ CANCELLED.xlsx
└── PIMA\ -\ INACTIVE.xlsx
```

#### Colorado

```
├── EX-003\ File\ Layout.xlsx
├── Master_Voting_History_List_\ Part1.txt.gz
├── Master_Voting_History_List_Voter_Details_\ Part1_11_30_2018_11_38_10.txt
├── README\ FILE_EX-003.doc
├── Registered_Voters_List_\ Part1.txt
└── SPLIT_DISTRICTS.zip
```

