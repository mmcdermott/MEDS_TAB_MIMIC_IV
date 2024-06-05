#!/usr/bin/env bash
# bash hf_cohort/hf_cohort_e2e.sh $TASK $OUTPUT_DIR $N_WORKERS

set -e

export $(cat .env | xargs)

METHOD=meds

TASK="$1"
OUTPUT_DIR="$2"
N_PARALLEL_WORKERS="$3"
WINDOW_SIZES="tabularization.window_sizes=[2h,12h,1d,7d,30d,365d,full]"
AGGS="tabularization.aggs=[static/present,code/count,value/count,value/sum,value/sum_sqd,value/min,value/max]"
MIN_CODE_FREQ=10

echo "Writing output to $OUTPUT_DIR/$TASK"

echo "Running identify_columns.py: Caching feature names and frequencies."
meds-tab-describe MEDS_cohort_dir=$MIMICIV_MEDS_DIR

echo "Running tabularize_static.py: tabularizing static data"
meds_tab-tabularize-static \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"

echo "Extracting task $TASK"
./aces_task_extraction.py
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS" \
    task_name=$TASK


POLARS_MAX_THREADS=1
LOG_DIR="$OUTPUT_DIR/$TASK/.logs"
mkdir -p $LOG_DIR
{ time \
    mprof run --include-children --exit-code --output "$LOG_DIR/mprofile.dat" \
        meds_tab-tabularize-time-series \
            --multirun \
            worker="range(0,$N_PARALLEL_WORKERS)" \
            hydra/launcher=joblib \
            MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
            tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" do_overwrite=False \
            "$WINDOW_SIZES" "$AGGS" \
    2> $LOG_DIR/cmd.stderr
} 2> $LOG_DIR/timings.txt

mprof plot -o $LOG_DIR/mprofile.png $LOG_DIR/mprofile.dat
mprof peak $LOG_DIR/mprofile.dat > $LOG_DIR/peak_memory_usage.txt


echo "Running task_specific_caching.py: tabularizing static data"
meds_tab-cache-task \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
    task_name=$TASK \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"

echo "Running xgboost"
meds_tab-xgboost \
    MEDS_cohort_dir=$MIMICIV_MEDS_DIR \
    task_name=$TASK \
    tabularization.min_code_inclusion_frequency="$MIN_CODE_FREQ" "$WINDOW_SIZES" do_overwrite=False "$AGGS"