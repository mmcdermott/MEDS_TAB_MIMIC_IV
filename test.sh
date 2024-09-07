#!/usr/bin/env bash
# bash -i test.sh

set -e

export POLARS_MAX_THREADS=1
MEDS_TAB_N_PARALLEL_WORKERS=64

MEDS_TAB_ENV=meds_tab
MIMICIV_MEDS_DIR=/storage/shared/meds_tabular_ml/mimiciv_example
MIMICIV_MEDS_TAB_COHORT_DIR="${MIMICIV_MEDS_DIR}"
MIMICIV_ALL_TASKS="readmission/30d,"
# mortality/in_icu/first_24h,mortality/in_icu/first_48h,mortality/in_hospital/first_24h,mortality/in_hospital/first_48h,mortality/post_hospital_discharge/30d,mortality/post_hospital_discharge/1_year,length_of_stay/in_hospital/first_24h/no_more_than_3d,length_of_stay/in_hospital/first_48h/no_more_than_3d,length_of_stay/in_icu/first_24h/no_more_than_3d,length_of_stay/in_icu/first_48h/no_more_than_3d"
CONDA_PATH=/storage/shared/miniforge3/
ACES_CONDA_ENV_NAME=aces_env

MEDS_TAB_DIR=/home/teya/MEDS_Tabular_AutoML/
MEDS_TAB_MIMIC_DIR=/home/teya/MEDS_TAB_MIMIC_IV/


cd ${MEDS_TAB_MIMIC_DIR}
echo "Activating environment $MEDS_TAB_ENV"
conda activate $MEDS_TAB_ENV
bash run.sh "${MIMICIV_ALL_TASKS}" $MEDS_TAB_N_PARALLEL_WORKERS $MIMICIV_MEDS_DIR $MIMICIV_MEDS_TAB_COHORT_DIR $CONDA_PATH $ACES_CONDA_ENV_NAME