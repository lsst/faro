`ci_hsc_gen3/DATA/hack_1/20200713T23h40m26s/matchedCatalog/0/68`
I chose the above to start with because it's on the edge and doesn't have a lot of sources.

The process I chose was to copy the matched catalogs for r and i from the above patch.
These are single band single patch matched catalogs.
I used `gzip` to compress them and read them in directly using `SimpleCatalog` in the test.

I also copied the persisted `Measurement` objects for the corresponding bands and patches.
These can be read in using `yaml.load`.
