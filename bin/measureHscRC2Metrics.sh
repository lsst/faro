#!/usr/bin/env bash

set -e

pipetask --long-log run -j $NUMPROC -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#singleFrame' -i HSC/RC2/defaults --register-dataset-types -o jenkins/singleFrame

pipetask --long-log run -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#jointcal' -i HSC/RC2/defaults,jenkins/singleFrame --register-dataset-types -o jenkins/jointcal

pipetask --long-log run -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#fgcm' -i HSC/RC2/defaults,jenkins/singleFrame --register-dataset-types -o jenkins/fgcm

pipetask --long-log run -j $NUMPROC -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#faro_singleFrame' -i jenkins/fgcm,jenkins/jointcal --register-dataset-types -o jenkins/faro_singleFrame

pipetask --long-log run -j $NUMPROC -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -d "tract = 9813 and skymap = 'hsc_rings_v1' AND patch in (40)" -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#step2' -i jenkins/fgcm,jenkins/jointcal --register-dataset-types -o jenkins/coadds

pipetask --long-log run -j $NUMPROC -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -d "tract = 9813 AND skymap = 'hsc_rings_v1' AND patch in (40)" -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#step3' -i jenkins/coadds --register-dataset-types -o jenkins/objects

pipetask --long-log run -j $NUMPROC -b ${RC2_SUBSET_DIR}/SMALL_HSC/butler.yaml -p ${RC2_SUBSET_DIR}'/pipelines/DRP.yaml#faro_coadd' -i HSC/RC2/defaults,jenkins/singleFrame,jenkins/fgcm,jenkins/jointcal,jenkins/coadds,jenkins/objects --register-dataset-types -o jenkins/faro_coadd

make_job_document.py ${RC2_SUBSET_DIR}/SMALL_HSC jenkins/faro_singleFrame
make_job_document.py ${RC2_SUBSET_DIR}/SMALL_HSC jenkins/faro_coadd
