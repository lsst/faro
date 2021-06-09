This is a place to put ``faro`` pipeline yaml files.

If you are interested in calculating all available metrics, run one of the following (depending whether you would like to apply external (jointcal, FGCM) calibrations to the matched visit catalogs or not):

- metrics_pipeline.yaml
- metrics_pipeline_jointcal_fgcm.yaml

Those "full" pipelines are aggregations of the other pipelines in this top-level directory. If you are only interested in a subset of available ``faro`` metrics, individual pipelines can be executed on their own.

Note that most of the ``metrics_pipeline*.yaml`` pipelines are themselves combinations of multiple steps. The "standard" steps in a ``faro`` pipeline are _preparation_ of the dataset (e.g., matching sources from all visits), _measurement_ of metrics, and _summary_ via aggregation of the measurements to create a single global metric summary value.
