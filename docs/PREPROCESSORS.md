# Preprocessors

Reggie preprocessors take the disparate file structures and formats of state's exports of voter information (voter file and voter history), and combines then into a single CSV-type file for easier handling.

## Creating preprocessors

1. Make sure to create a configuration file for the state.  See [STATE-CONFIGS.md](./STATE-CONFIGS.md).
2. Create new file in `reggie/ingestion/preprocessor/` as `STATE-ID.py`.
3. Create a class with name `PreprocessorStateName` which will inherit from Reggie's `Preprocessor`.  It should implement the following:
   1. The *constructor* (`__init__`) method should ??.  It will have the following parameters:
      1. `self`: The object itself
      2. `raw_s3_file`: ??
      3. `config_file`: ??, 
      4. `force_date` (defaults to `None`): ??
      5. `**kwargs`: Any other parameters that may be passed along.
   2. Implement the `execute` method.  This will be specific to the state and data.
      1. Create a `FileItem` as `self.processed` which will be the processed CSV file.  For example, if you have created a Dataframe, `df_voters`:
         ```
            self.processed_file = FileItem(
               name="{}.processed".format(self.config["state"]),
               io_obj=StringIO(df_voters.to_csv(encoding="utf-8", index=True)),
               s3_bucket=self.s3_bucket,
           )
         ```
      2. The `execute` method should also create a dict assigned to `self.meta` with the follow properties:
         1. `message`: A string that defines the specific file, i.e. something like `[state]_[current_date]`
         2. `array_encoding`: (optional for voter history) A string version of a dict where each property is the election identifier, and each value is a dict with the following values:
            1. `index`: The index of the value, as associated with the `array_decoding` list.
            2. `date`: The date of the election.
            3. `count`: The number of voters that voted in the election.
         3. `array_decoding`: (optional for voter history) A string version of a list of all the election identifiers.
4. Add to `reggie/ingestion/preprocesser/state_router.py`
   1. Add the import of the processor class.
   2. Add to the `routes` dictionary.