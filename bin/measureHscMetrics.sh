#!/usr/bin/env bash
sh hsc_gen2_to_gen3.sh

pipetask run -j $NUMPROC -b validation_hsc_gen3/butler.yaml --register-dataset-types -p $METRIC_PIPELINE_TASKS_DIR/pipelines/validate_drp_metrics_pipeline.yaml -d "" -o jenkins/validate_drp_metrics_all -i shared/valid_hsc_all,skymaps 

make_job_document.py validation_hsc_gen3 jenkins/validate_drp_metrics_all
