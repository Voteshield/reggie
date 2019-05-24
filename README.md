# Veggie

Veggie is a python package for reading state voter registration files created by Voteshield, a data analytics tool designed to protect the integrity of American elections by monitoring changes to the voter rolls.

![](veggie_example.gif)

## Using Veggie

Veggie will take your voter file and convert it into a single compressed voter file along with voter history metadata. You can use it on the command line or within Python by giving the location of the file, the state, and the date of the file snapshot. The current available states are Arizona, Colorado, Florida, Georgia, Iowa, Kansas, Michigan, Minnesota, Missouri, Nevada, New Jersey, New York, North Carolina, Ohio, Pennsylvania, and Texas.

## Examples

```python
from veggie import convert_voter_file

nc_dataframe, metadata = convert_voter_file(state='north_carolina',
                                            local_file='nc_2018-12-22.zip',
                                            date='2018-12-22')
```

```bash
veg --state north_carolina --local_file nc_2018-12-22.zip --date 2018-12-22
```

Warning: as voter files are quite large, veggie may take significant time and memory. Internally, Voteshield uses AWS instead of performing these jobs locally. Another option is to use [Colab](https://colab.research.google.com/) to perform larger jobs. 

## What is a Voter File?

Voter files, often referred to as voter registration lists, are files kept by state governments to determine which citizens are registered and eligible for elections. Each state keeps a voter file, has different information about each voter (sometimes keeping race, gender, email, phone number, address, and other information), charges a different amount for this public file (Ohio and North Carolina are free, while some states charge more than $10k for a file), and also keeps these files in different formats. These issues make it frustratingly difficult to get insight into how a state deals with their voters' data, and also laborious to read into analytics software like R/Python/Stata. 

## Voter History

Many states keep voter history in their voter file, often kept in a separate file at the county level. Veggie will combine each of these files, and group the history by turning the collection of voter history into a column in the voter registration file called all_history. This will either be kept in an array of vote slugs, or a collection of indicies that correspond to elections held in the returned metadata.  

### Voter Slug Example

{"STATE-GENERAL ELECTION 11/06/2007","GENERAL ELECTION 11/04/2008","STATE GENERAL 2010 11/02/2010"}  

This represents the voter history for a single voter in New Jersey who voted in a state general election in 2007, voted in the general in 2008, and also in the general in 2010. 


## Installation 

```bash
$ pip install veggie
```


## Expected File Format by State

Veggie automatically decompresses all of the files in the provided file, and expects voter files in the format that the individual states use.