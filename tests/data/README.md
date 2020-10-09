`ci_hsc_gen3/DATA/hack_2/20200717T19h08m37s/matchedCatalog/0/68`
I chose the above to start with because it has non-NaN values for all the measurements I wanted to test.

The process I chose was to copy the matched catalogs for r and i from the above patch.
These are single band single patch matched catalogs.
I used `gzip` to compress them and read them in directly using `SimpleCatalog` in the test.

The script `shrink_cat.py` can be used to further reduce test data size.
It takes the input catalog as the first argument and the output catalog as the second.
It has a list of field names that should be preserved and attempts to set all others to zero.
Flags are read only, but should compress well anyway.

I also copied the persisted `Measurement` objects for the corresponding bands and patches.
These can be read in using `yaml.load`.
