# Reggie state configs

State configuration files are used to describe the state as well as how we ingest the raw voter data and turn it into more usable data.

## Location

State config files are found in the `reggie/configs/data/` directory and are in YAML format.

## Required fields

The following fields are required in the config for Reggie:

- ??

The following fields are required in the config for VoteShield:

- `voter_status`
- `birthday_identifier`
- `party_identifier`
- ??

## State config

### State basics

Values that describe basics about the state itself.

| Field          | Type   | Values | Description                                       | Example    |
| -------------- | ------ | ------ | ------------------------------------------------- | ---------- |
| `state`        | string |        | The ID of the state.                              | `new_york` |
| `abbreviation` | string |        | The two-letter, lowercased abbreviation of state. | `ny`       |

## Vote file and history package

### File basics

| Field                    | Type    | Values | Description | Example                                                                   |
| ------------------------ | ------- | ------ | ----------- | ------------------------------------------------------------------------- | ------ |
| `format.segmented_files` | boolean | `true  | false`      | Affects download ???                                                      | `true` |
| `format.separate_hist`   | boolean | `true  | false`      | History is a separate file from the voter file. (No reference in code???) | `true` |

## Voter file config

### Voter file basics

Values that describe the voter file.

| Field                           | Type    | Values      | Description                                                 | Example                                               |
| ------------------------------- | ------- | ----------- | ----------------------------------------------------------- | ----------------------------------------------------- | ------------ |
| `file_type`                     | string  | ???         | The type of file it is.                                     | `txt`                                                 |
| `delimiter`                     | string  |             | For CSV-like files, the delimiting character .              | `"                                                    | "`           |
| `has_headers`                   | boolean | `true       | false`                                                      | For CSV-like files, whether the first row is headers. | `true`       |
| `fixed_width`                   | boolean | `true       | false`                                                      | Whether the file is a fixed-width formatted file.     | `true`       |
| `file_class`                    | string  | `voter_file | ???`                                                        | ?????                                                 | `voter_file` |
| `source`                        | string  | `boe        | ???`                                                        | ????? (required)                                      | `boe`        |
| `expected_number_of_files`      | number  |             | The number of files to be expected from the source.         | `3`                                                   |
| `expected_number_of_hist_files` | number  |             | The number of history files to be expected from the source. | `3`                                                   |

### General data parsing

Values around general data parsing.

| Field         | Type                    | Values | Description                                                                       | Example      |
| ------------- | ----------------------- | ------ | --------------------------------------------------------------------------------- | ------------ |
| `date_format` | string, list of strings |        | The parsing string as defined by ??? module. Possible values are documented here? | `"%m/%d/%Y"` |

### Voter identifiers

Fields that help describe voter identifiters.

| Field          | Type   | Values | Description                                                                                                                                                          | Example                                |
| -------------- | ------ | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `voter_id`     | string |        | The name of the column that defines the voter identifier.                                                                                                            | `ID_VOTER`                             |
| `match_fields` | list   |        | List of the names of the columns that can be used to reasonable match records up when voter ID fails. In general, this should be first and last name, and birthdate. | `- First`<br>`- Last`<br>`- Birthdate` |

### Locales

Configuration around locales.

| Field                       | Type    | Values | Description                                                                          | Example                                                                                     |
| --------------------------- | ------- | ------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ------- |
| `county_identifier`         | string  |        | The name of the column that defines the county identifier.                           | `County_Name`                                                                               |
| `primary_locale_identifier` | string  |        | The name of the column that defines the primary locale, which is usually the county. | `County_Name`                                                                               |
| `numeric_primary_locale`    | boolean | `true  | false`                                                                               | Whether the primary locale column is a number column and we should coerce it into a number. | `false` |
| `precinct_identifier`       | string  |        | The name of the column that identifies the precinct ID.                              | `County_Name`                                                                               |

**NOTE**: Identifiers should NOT be coerced into numbers and should remain strings.

### Voter status

Fields that define voter status information in the file.

| Field                    | Type            | Values | Description                                                                                                                                                     | Example     |
| ------------------------ | --------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `voter_status`           | string          |        | The name of the column that defines the voter status. Note that if it doesn't exist, create a dummy column in the `columns` and `column_names` and use that.    | `Status`    |
| `voter_status_active`    | string          |        | The values in the file that defines when a voter is Active.                                                                                                     | `Active`    |
| `voter_status_inactive`  | string          |        | The values in the file that defines when a voter is Inactive.                                                                                                   | `Inactive`  |
| `voter_status_pending`   | string          |        | If the state supports "pending" voters, the values in the file that defines when a voter is Inactive.                                                           | `Pending`   |
| `voter_status_cancelled` | string          |        | If the state supports "cancelled" voters, the values in the file that defines when a voter is Inactive.                                                         | `Cancelled` |
| `cancel_data_fields`     | list of strings |        | If the state supports "cancelled" voters, and you want to preserve any of the data in those cancelled records such as reason codes, list of those column names. | `Cancelled` |

### Political parties

Configuration around political parties.

| Field                  | Type   | Values | Description                                                                                                                                                                    | Example             |
| ---------------------- | ------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------- |
| `party_identifier`     | string |        | The name of the column that defines the registered party of the voters. Note that if it doesn't exist, create a dummy column in the `columns` and `column_names` and use that. | `party_affiliation` |
| `democratic_party`     | string |        | The name of the value(s) that defines the Democratic party.                                                                                                                    | `DFL`               |
| `republican_party`     | string |        | The name of the value(s) that defines the Democratic party.                                                                                                                    | `GOP`               |
| `libertarian_party`    | string |        | The name of the value(s) that defines the Libertarian party.                                                                                                                   | `lib`               |
| `no_party_affiliation` | string |        | The name of the value(s) that defines the Democratic party.                                                                                                                    | `Unaffiliated`      |

### Demographics

Configuration around demographic data.

| Field                 | Type   | Values | Description                                                                                                                                                          | Example      |
| --------------------- | ------ | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `birthday_identifier` | string |        | The name of the column that defines the voters data of birth. Note that if it doesn't exist, create a dummy column in the `columns` and `column_names` and use that. | `birth_date` |

### Voter name

Fields that help describe the voters' names.

| Field                  | Type | Values | Description                                                                                             | Example                                           |
| ---------------------- | ---- | ------ | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `name_fields`          | list |        | List of the names of the columns that defines the voter full name.                                      | `- First`<br>`- Middle`<br>`- Last`<br>`- Suffix` |
| `standard_name_fields` | list |        | List of the names of the columns that defines just the first, middle, and last part of the voters name. | `- First`<br>`- Middle`<br>`- Last`               |

### Addresses

Fields that help describe the voters' addresses.

| Field            | Type | Values | Description                                                                                                                              | Example                                                     |
| ---------------- | ---- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `address_fields` | list |        | List of the names of the columns that defines the address(es) for the voter, which could include both residential and mailing addresses. | `- Street Number`<br>`- Street Name`<br>`- City`<br>`- Zip` |

### Voter file columns

Fields that describe the columns in the voter file.

| Field             | Type       | Values | Description                                                                                                                                     | Example                                          |
| ----------------- | ---------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `column_names`    | list       |        | Complete list of the names of the all the columns.                                                                                              | `- voter_id`<br>`- first_name`<br>...            |
| `columns`         | dictionary |        | Complete dictionary that maps the column names from `column_names` to the data type which should be `text`, `int`, `date`, `timestamps`, or ??. | `voterid: character`<br>`birthdate: date`<br>... |
| `ordered_columns` | list       |        | Complete list of the column names from `column_names` in the order they will go into the database table ??.                                     | `- voter_id`<br>`- first_name`<br>...            |

## Modifications configs

### Setup

| Field          | Type       | Values | Description                                                                                                                                                                                  | Example                                                                                                                                                |
| -------------- | ---------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `primary_key`  | list       |        | List of names of columns. Defines the modifications table primary key; should contain `pre_date`, `post_date`, Voter ID column, and `change_type`.                                           | `- pre_date`<br>`- post_date`<br>`- Voter ID`<br>`- change_type`                                                                                       |
| `base_columns` | dictionary |        | Dictionary that maps column names to data types for the modifications table; should contain `pre_date`, `post_date`, Voter ID column, and `change_type`, `pre_value`, `post_value`, `state`. | `pre_date: timestamp`<br>`post_date: timestamp`<br>`Voter ID: text`<br>`change_type: text`<br>`pre_value: text`<br>`post_value: text`<br>`state: text` |

## Summary configs

### Setup

| Field             | Type | Values | Description                                                                                                                                                                                                                                                                                             | Example                                                                                                                                                                     |
| ----------------- | ---- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `summary_columns` | list |        | List of names of columns. Defines the summary table columns and should contain `totals`, `date`, `state`, `locale`, age columns (`ages_18_34`, `ages_35_49`, `ages_50_64`, and `ages_65_130`), gender columns if available in data (`male`, `female`), and then any party codes as defined in the data. | `- totals`<br>`- date`<br>`- state`<br>`- locale`<br>`- ages_18_34`<br>`- ages_35_49`<br>`- ages_50_64`<br>`- ages_65_130`<br>`- democratic`<br>`- republican`<br>`- green` |
| `summary_types`   | list |        | List that matches `summary_columns` to data types, where it could be `text`, `timestamp`, `date`, or `int`.                                                                                                                                                                                             | `- int`<br>`- timestamp`<br>`- text`<br>`- text`<br>`- int`<br>`- int`<br>`- int`<br>`- int`<br>`- int`<br>`- int`<br>`- int`                                               |

## Voter history configs

These are necessary if there is voter history information.

### Voter history columns

| Field                       | Type       | Values | Description                                                                                                                                                                                                                                                                                                                            | Example                                                   |
| --------------------------- | ---------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `hist_columns`              | list       |        | List of the name of columns from the voter history data.                                                                                                                                                                                                                                                                               | `- voterid`<br>`- date`<br>...                            |
| `hist_columns_type`         | dictionary |        | Dictionary that maps the column names in `hist_columns` to data types such as `text`, `timestamp`, `date`, or `int`                                                                                                                                                                                                                    | `voterid: text`<br>`date: timestamp`<br>...               |
| `generated_columns`         | dictionary |        | Dictionary that maps new columns generated for voter history to their data types. This should include `all_history`, which is an array of election identifiters, `sparse_history` which is an array of index to an election (assuming all elections across the history file), as well as other aggregate fields from the history data. | `all_history: text[]`<br>`- sparse_history: int[]`<br>... |
| `ordered_generated_columns` | list       |        | List of name of columns from `generated_columns` for ordering.                                                                                                                                                                                                                                                                         | `- all_history`<br>`- sparse_history`<br>...              |

## Preprocess config

### Generated columns

| Field                       | Type       | Values | Description                                                                                                                 | Example                                                   |
| --------------------------- | ---------- | ------ | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `generated_columns`         | dictionary |        | Dictionary that maps new columns generatored from Reggie (preprocess) to their data types.                                  | `all_history: text[]`<br>`- sparse_history: int[]`<br>... |
| `ordered_generated_columns` | list       |        | List of the name of new columns generatored from Reggie (preprocess) in the order they will be added to the database table. | `- all_history`<br>`- sparse_history`<br>...              |

## Ballot or voting config

| Field                  | Type   | Values | Description                               | Example    |
| ---------------------- | ------ | ------ | ----------------------------------------- | ---------- |
| `absentee_ballot_code` | string |        | The code used to define an absentee vote. | `absentee` |

## Customized config

You can also add custom config that may be used in Reggie or VoteShield's processing. Make sure to document what it does and why.
