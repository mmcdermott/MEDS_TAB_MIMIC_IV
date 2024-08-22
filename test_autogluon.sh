#!/usr/bin/env bash
# bash -i test_autogluon.sh

set -e

export POLARS_MAX_THREADS=1
MEDS_TRANSFORM_N_PARALLEL_WORKERS=16
MEDS_TAB_N_PARALLEL_WORKERS=32

MEDS_TAB_ENV=autogluon
MIMICIV_RAW_DIR=/storage/shared/mimiciv/dataset/mimiciv/2.0 
MIMICIV_PREMEDS_DIR=/storage/shared/meds_tabular_ml/mimiciv_v20/premeds
MIMICIV_MEDS_DIR=/storage/shared/meds_tabular_ml/mimiciv_v20/meds
MIMICIV_MEDS_TASK_DIR=/storage/shared/meds_tabular_ml/mimiciv_v20/tasks
MIMICIV_MEDS_TAB_COHORT_DIR=/storage/shared/meds_tabular_ml/mimiciv_v20/tabularize
MIMICIV_ALL_TASKS="readmission/30d"
TASKS_DIR=/storage/shared/meds_tabular_ml/mimiciv_v20/tasks
CONDA_PATH=/storage/shared/miniforge3/
ACES_CONDA_ENV_NAME=aces_env

MEDS_TRANSFORM_DIR=/home/nassim/projects/MEDS_transforms
MEDS_TRANFORM_ENV=meds_transform

MEDS_TAB_DIR=/home/nassim/projects/MEDS_Tabular_AutoML/
MEDS_TAB_MIMIC_DIR=/home/nassim/projects/MEDS_TAB_MIMIC_IV/


# if ! conda env list | grep -q "^$MEDS_TRANFORM_ENV "; then
#     conda create -n $MEDS_TRANFORM_ENV python=3.12 -y
# else
#     echo "Environment $MEDS_TRANFORM_ENV already exists."
# fi

# conda activate $MEDS_TRANFORM_ENV

# pip install MEDS-transforms==0.0.5 hydra-joblib-launcher

# conda activate $MEDS_TRANFORM_ENV
# cd $MEDS_TRANSFORM_DIR
# bash ./MIMIC-IV_Example/joint_script.sh $MIMICIV_RAW_DIR $MIMICIV_PREMEDS_DIR $MIMICIV_MEDS_DIR $MEDS_TRANSFORM_N_PARALLEL_WORKERS

# MEDS_transform-reshard_to_split \
#   --multirun \
#   worker="range(0,$MEDS_TAB_N_PARALLEL_WORKERS)" \
#   hydra/launcher=joblib \
#   input_dir="$MIMICIV_MEDS_DIR" \
#   cohort_dir="$MIMICIV_MEDS_TAB_COHORT_DIR" \
#   'stages=["reshard_to_split"]' \
#   stage="reshard_to_split" \
#   stage_configs.reshard_to_split.n_patients_per_shard=2500

cd ${MEDS_TAB_MIMIC_DIR}
conda activate $MEDS_TAB_ENV
bash autogluon.sh "${MIMICIV_ALL_TASKS}" $MEDS_TAB_N_PARALLEL_WORKERS $MIMICIV_MEDS_TAB_COHORT_DIR $MIMICIV_MEDS_TAB_COHORT_DIR $CONDA_PATH $ACES_CONDA_ENV_NAME