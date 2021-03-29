#!/usr/bin/env sh

set -e

mkdir temp_gen2repo
echo lsst.obs.hsc.HscMapper > temp_gen2repo/_mapper
ingestImages.py temp_gen2repo/ $VALIDATION_DATA_HSC_DIR/raw/*
ln -s $VALIDATION_DATA_HSC_DIR/ref_cats temp_gen2repo/
butler convert --gen2root temp_gen2repo/ --calibs $VALIDATION_DATA_HSC_DIR/CALIB/ validation_hsc_gen3 -C $FARO_DIR/config/hsc_convert.py
butler register-skymap validation_hsc_gen3/ -C $FARO_DIR/config/hsc_sky_map.py

pipetask --long-log run -j $NUMPROC -b validation_hsc_gen3/butler.yaml -i HSC/calib,HSC/raw/all,refcats --register-dataset-types -p "${PIPE_TASKS_DIR}/pipelines/DRP.yaml:processCcd" -d "visit in (903332, 903340, 903982, 904006, 904350, 904378, 904828, 904846) and detector < 104 and detector != 9" -o shared/valid_hsc_all -c isr:doStrayLight=False --instrument lsst.obs.subaru.HyperSuprimeCam
