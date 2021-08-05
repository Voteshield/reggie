# Managing data for WV tests

## Zipping

Note that we keep the original files and the zips to make it easier to edit and manage, though it is a bit redundant. To create a new zip file easily:

- Basic: `cd tests/data/west_virginia/basic && zip -r WV-TEST.zip ./; cd -;`
- No gender: `cd tests/data/west_virginia/no_gender && zip -r WV-TEST-no-gender.zip ./; cd -;`
