# Reggie

Reggie is a command-line tool and Python package for parsing voter registration files in the US.  Reggie will convert raw voter files into a single voter file that includes voter history data if available.

Reggie is created by [VoteShield](https://votehshield.us/), a data analytics tool designed to protect the integrity of American elections by monitoring changes to the voter rolls.

## Background

Voter registration files are maintained in almost every state, and though they contain different data, each one usually includes basic data about every registered voter in that state and usually includes a voter history which usually includes last or each time they voted.

Each state has it's own laws, systems, and processes on how they manage, store, define, and allow access to their voter registration files.

## Supported states

See the `reggie/configs/` directory for a possibly more accurate list of states that are currently supported, but here is the current list with identifiers:

| State          | Identifier       | Notes                            |
| -------------- | ---------------- | -------------------------------- |
| Arizona        | `arizona`        | _TODO: Note about `arizona2`_    |
| Colorado       | `colorado`       |                                  |
| Florida        | `florida`        |                                  |
| Georgia        | `georgia`        |                                  |
| Iowa           | `iowa`           |                                  |
| Kansas         | `kansas`         |                                  |
| Michigan       | `michigan`       |                                  |
| Minnesota      | `minnesota`      |                                  |
| Missouri       | `missouri`       |                                  |
| Nevada         | `nevada`         |                                  |
| New Jersey     | `new_jersey`     | _TODO: Note about `new_jersey2`_ |
| New York       | `new_york`       |                                  |
| North Carolina | `north_carolina` |                                  |
| Ohio           | `ohio`           |                                  |
| Pennsylvania   | `pennsylvania`   |                                  |
| Texas          | `texas`          |                                  |

## How it works

Many states keep voter history in their voter file, often kept in a separate file at the county level. Reggie will combine each of these files, and group the history by turning the collection of voter history into a column in the voter registration file called all_history. The specifics of how this should be done can be found [here](https://github.com/Voteshield/reggie/wiki/UVFF-and-State-Onboarding).

## Usage

### Python

```python
from reggie import convert_voter_file

nc_dataframe, metadata = convert_voter_file(state='north_carolina',
                                            local_file='nc_2018-12-22.zip',
                                            date='2018-12-22')
```
### Command Line

The command `reg` is provided by the package.  Utilize `reg --help` to get help on options.

```bash
$ reg --state north_carolina --local_file nc_2018-12-22.zip --file_date 2018-12-22
```

## Installation

You can install Reggie as a git resource.

```
pip install git+https://github.com/Voteshield/reggie.git
```

## Contributing and development

### Get code and install

Reggie requires Python 3.6 or greater.  All commands are assumed to be within a terminal or command line application.

1. Checkout or download the code from [Github](https://github.com/Voteshield/reggie).
2. Make sure to go into the project directory: `$ cd reggie`
3. (optional, though suggested) Create and utilize a [virtual environment](https://docs.python.org/3/tutorial/venv.html).
4. Install dependencies: `$ pip install -e .`

### Adding and managing a state

See the [wiki](https://github.com/Voteshield/reggie/wiki/UVFF-and-State-Onboarding) for more detailed information.  The short version is that a state has two parts:

1. The state config file  See [docs/STATE-CONFIGS.md](./docs/STATE-CONFIGS.md).
2. The state preprocessor.  See [docs/PREPROCESSORS.md](./docs/PREPROCESSORS.md).

### Tests

To run tests, use the following command: `$ python -m pytest`

Note that because the voter file for North Carolina is entirely public and can be found on the [state's website](https://dl.ncsbe.gov/index.html?prefix=data/), a smaller version of the NC file is included in the `reggie/test_data/` directory. 