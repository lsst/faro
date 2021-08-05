The metrics pipeline example can be run by setting up the standard Stack,
currently tested using `w_2020_16`. Make sure that you have a realized version
of [`ci_hsc_gen3`](https://github.com/lsst/ci_hsc_gen3) set up using the
corresponding weekly tag, e.g., `w.2020.16`. Note that the gen3 butler is still
changing so fast that it's likely you will need to rerun `scons`
in `ci_hsc_gen3` weekly. At the time of writing, this package was not an `eups`
package, so you'll have to add the `metric-pipeline-tasks/tasks` directory to
your python path by hand.

`> export PYTHONPATH=$PYTHONPATH:$PWD`

You will also have to setup the $METRIC\_PIPE\_DIR environment variable this
variable should point to the root of the `metric-pipeline-tasks` repository.

Next, from the `metric-pipeline-tasks` directory, you can execute the full
pipeline with the command

`pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -p pipelines/metrics_pipeline.yaml -d "band = 'r'" -o pipeTest -i shared/ci_hsc_output`

or, if running the pipeline again, use the command

`pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -p pipelines/metrics_pipeline.yaml -d "band = 'r'" -o pipeTest --replace-run`

You can execute individual tasks in the pipeline with commands like

`pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i shared/ci_hsc_output --register-dataset-types -t MatchedCatalogs.MatchedCatalogTask -d "band = 'r'" -o matchedTest`

Enjoy!
