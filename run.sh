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
WINDOW_LIST="[2h,12h,1d,7d,30d,365d,full]"
WINDOW_SIZES="tabularization.window_sizes=${WINDOW_LIST}"
AGG_LIST=[static/present,code/count,value/count,value/sum,value/sum_sqd,value/min,value/max]
AGGS="tabularization.aggs=${AGG_LIST}"
MIN_CODE_FREQ=10

IFS=',' read -r -a TASK_ARRAY <<< "$TASKS"

# describe codes
echo "Describing codes"
meds-tab-describe \
    MEDS_cohort_dir="$MIMICIV_MEDS_DIR" output_cohort_dir="$MEDS_TAB_COHORT_DIR"

echo "Tabularizing static data"
echo meds-tab-tabularize-static \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"

POLARS_MAX_THREADS=1
LOG_DIR="$MIMICIV_MEDS_DIR/.logs/tstab/"
mkdir -p $LOG_DIR
{ time \
    mprof run --include-children --exit-code --output "$LOG_DIR/mprofile.dat" \
        meds-tab-tabularize-time-series \
            --multirun \
            worker="range(0,$N_PARALLEL_WORKERS)" \
            hydra/launcher=joblib \
            MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR"\
            tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
            "$WINDOW_SIZES" "$AGGS" \
    2> $LOG_DIR/cmd.stderr
} 2> $LOG_DIR/timings.txt

meds-tab-tabularize-time-series \
    --multirun \
    worker="range(0,$N_PARALLEL_WORKERS)" \
    hydra/launcher=joblib \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
    "$WINDOW_SIZES" "$AGGS"

# Copy tasks to cohort directory
rsync -r tasks/ ${MEDS_TAB_COHORT_DIR}/tasks

TASK_DIR_LIST=()

for TASK in "${TASK_ARRAY[@]}"
do
  echo "Extracting task $TASK"
  bash -i aces_task_extraction.sh $CONDA_PATH $MIMICIV_MEDS_DIR $MEDS_TAB_COHORT_DIR ${MEDS_TAB_COHORT_DIR}/tasks $TASK $ACES_CONDA_ENV_NAME
  TASK_DIR_LIST+=("$MEDS_TAB_COHORT_DIR/tasks/$TASK")
done

echo "Running task_specific_caching.py: tabularizing static data"
IFS=,
echo "${TASK_ARRAY[*]}"
meds-tab-cache-task \
    --multirun \
    hydra/launcher=joblib \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
    task_name="${TASK_ARRAY[*]}" \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS" tqdm=True

for TASK in "${TASK_ARRAY[@]}"
do
  echo "Running xgboost"
  meds-tab-xgboost \
      --multirun \
      tabularization.window_sizes=$(generate-subsets $WINDOW_LIST) \
      tabularization.aggs=$(generate-subsets $AGG_LIST) \
      MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
      task_name=$TASK \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
      hydra.sweeper.direction=maximize model_saving.model_dir="$MEDS_TAB_COHORT_DIR/models/xgboost_window_agg_search/$TASK/"
  
  meds-tab-xgboost \
      --multirun \
      MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
      task_name=$TASK \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS" \
      hydra.sweeper.direction=maximize model_saving.model_dir="$MEDS_TAB_COHORT_DIR/models/xgboost_all/$TASK/"

#   # meds-tab-autogluon \
#   #   MEDS_cohort_dir=$MIMICIV_MEDS_DIR output_cohort_dir="$MEDS_TAB_COHORT_DIR" \
#   #   task_name=$TASK \
#   #   tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" tabularization.max_included_codes=10 "$WINDOW_SIZES" do_overwrite=False "$AGGS" \
#   #   hydra.sweeper.direction=maximize model_dir="$MEDS_TAB_COHORT_DIR/models/autogluon/test/$TASK/
# done
