#!/usr/bin/env bash
# bash hf_cohort/hf_cohort_e2e.sh $TASK $OUTPUT_DIR $N_WORKERS

set -e

export $(cat .env | xargs)

METHOD=meds

TASKS="$1"
OUTPUT_DIR="$2"
N_PARALLEL_WORKERS="$3"
WINDOW_SIZES="tabularization.window_sizes=[2h,12h,1d,7d,30d,365d,full]"
AGGS="tabularization.aggs=[static/present,code/count,value/count,value/sum,value/sum_sqd,value/min,value/max]"
MIN_CODE_FREQ=10

IFS=',' read -r -a TASK_ARRAY <<< "$TASKS"

#echo "Running identify_columns.py: Caching feature names and frequencies."
#meds-tab-describe MEDS_cohort_dir=$MIMICIV_MEDS_DIR

#echo "Running tabularize_static.py: tabularizing static data"
#meds-tab-tabularize-static \
#    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
#    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"

# POLARS_MAX_THREADS=1
# LOG_DIR="$MIMICIV_MEDS_DIR/.logs/tstab/"
# mkdir -p $LOG_DIR
# { time \
#     mprof run --include-children --exit-code --output "$LOG_DIR/mprofile.dat" \
#         meds-tab-tabularize-time-series \
#             --multirun \
#             worker="range(0,$N_PARALLEL_WORKERS)" \
#             hydra/launcher=joblib \
#             MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
#             tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
#             "$WINDOW_SIZES" "$AGGS" \
#     2> $LOG_DIR/cmd.stderr
# } 2> $LOG_DIR/timings.txt

meds-tab-tabularize-time-series \
    --multirun \
    worker="range(0,$N_PARALLEL_WORKERS)" \
    hydra/launcher=joblib \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
    "$WINDOW_SIZES" "$AGGS"

for TASK in "${TASK_ARRAY[@]}"
do
  echo "Extracting task $TASK"
  ./aces_task_extraction.py MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False \
      "$AGGS" task_name=$TASK

  echo "Running task_specific_caching.py: tabularizing static data"
  meds-tab-cache-task \
      MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
      task_name=$TASK \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"

  echo "Running xgboost"
  meds-tab-xgboost \
      MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
      task_name=$TASK \
      output_dir="$OUTPUT_DIR/$TASK" \
      tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"
done
