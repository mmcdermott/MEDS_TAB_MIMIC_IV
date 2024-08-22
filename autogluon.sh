#!/usr/bin/env bash
# bash run.sh $TASK $OUTPUT_DIR $N_WORKERS

set -e

export $(cat .env | xargs)

METHOD=meds

TASKS="$1"
N_PARALLEL_WORKERS="$2"
MIMICIV_MEDS_DIR="$3"
MEDS_TAB_COHORT_DIR="$4"
CONDA_PATH="$5"
ACES_CONDA_ENV_NAME="$6"
WINDOW_SIZES="tabularization.window_sizes=[2h,12h,1d,7d,30d,365d,full]"
AGGS="tabularization.aggs=[static/present,code/count,value/count,value/sum,value/sum_sqd,value/min,value/max]"
MIN_CODE_FREQ=10

IFS=',' read -r -a TASK_ARRAY <<< "$TASKS"

# echo "Running task_specific_caching.py: tabularizing static data"
# IFS=,
# echo "${TASK_ARRAY[*]}"
# meds-tab-cache-task \
#     MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
#     task_name="${TASK_ARRAY[0]}" \
#     tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS" tqdm=True

for TASK in "${TASK_ARRAY[@]}"
do
  echo "Running autogluon"
  meds-tab-autogluon \
      MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
      task_name=$TASK \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" tabularization.max_included_codes=10 "$WINDOW_SIZES" do_overwrite=False "$AGGS" \
      hydra.sweeper.direction=maximize
done
