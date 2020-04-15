The matched catalog task can be run by setting up the standard stack, I'm using `w_2020_13`.
Make sure you have a realized version of `ci_hsc_gen3` set up.
Note that the gen3 butler is still changing so fast that it's likely you will need to rerun `scons` in `ci_hsc_gen3` weekly.
At the time of writing, this package was not an `eups` package, so you'll have to add this directory to your python path by hand.

`> export PYTHONPATH=$PYTHONPATH:$PWD`

Then you can execute the matcher with something like:

`pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i shared/ci_hsc_output --register-dataset-types -t MatchedCatalogs.MatchedCatalogTask -d "abstract_filter = 'r'" -o matchedTest`

Enjoy!