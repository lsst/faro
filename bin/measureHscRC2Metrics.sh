#!/usr/bin/env bash

set -e

pipetask --long-log run -j $NUMPROC -b SMALL_HSC/butler.yaml -d "detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322)" -p 'pipelines/DRP.yaml#singleFrame' -i HSC/RC2/defaults --register-dataset-types -o jenkins/singleFrame

pipetask --long-log run -b SMALL_HSC/butler.yaml -d "band in ('g', 'r', 'i', 'z', 'y') AND detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322)" -p 'pipelines/DRP.yaml#jointcal' -i HSC/RC2/defaults,jenkins/singleFrame --register-dataset-types -o jenkins/jointcal

pipetask --long-log run -b SMALL_HSC/butler.yaml -d "detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322)" -p 'pipelines/DRP.yaml#fgcm' -i HSC/RC2/defaults,jenkins/singleFrame --register-dataset-types -o jenkins/fgcm

pipetask --long-log run -j $NUMPROC -b SMALL_HSC/butler.yaml -p 'pipelines/DRP.yaml#faro_singleFrame' -i HSC/RC2/defaults,jenkins/singleFrame,jenkins/fgcm,jenkins/jointcal --register-dataset-types -o jenkins/faro_singleFrame

pipetask --long-log run -j $NUMPROC -b SMALL_HSC/butler.yaml -d "tract = 9813 and skymap = 'hsc_rings_v1' AND detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322)" -p 'pipelines/DRP.yaml#step2' -i HSC/RC2/defaults,jenkins/singleFrame,jenkins/fgcm,jenkins/jointcal --register-dataset-types -o jenkins/coadds

pipetask --long-log run -j $NUMPROC -b SMALL_HSC/butler.yaml -d "tract = 9813 AND skymap = 'hsc_rings_v1' AND patch in (38, 39, 40, 41) AND detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322)" -p 'pipelines/DRP.yaml#step3' -i HSC/RC2/defaults,jenkins/singleFrame,jenkins/fgcm,jenkins/jointcal,jenkins/coadds --register-dataset-types -o jenkins/objects

pipetask --long-log run -j $NUMPROC -b SMALL_HSC/butler.yaml -p 'pipelines/DRP.yaml#faro_coadd' -i HSC/RC2/defaults,jenkins/singleFrame,jenkins/fgcm,jenkins/jointcal,jenkins/coadds,jenkins/objects --register-dataset-types -o jenkins/faro_coadd

make_job_document.py SMALL_HSC jenkins/faro_singleFrame
make_job_document.py SMALL_HSC jenkins/faro_coadd
