#!/usr/bin/env bash

CONDA_PATH=$1
MEDS_DIR=$2
DATA_DIR=$3
TASK_COHORT_DIR=$4
TASK_NAME=$5
CONDA_ENV_NAME=$6

if ! conda env list | grep -q "^$CONDA_ENV_NAME "; then
    conda create -n $CONDA_ENV_NAME python=3.12 -y
else
    echo "Environment $CONDA_ENV_NAME already exists."
fi

conda activate $CONDA_ENV_NAME
ENV_BIN_DIR="$CONDA_PREFIX/bin"

echo $ENV_BIN_DIR/pip install es-aces==0.5.0 hydra-joblib-launcher

echo PATH="$CONDA_PATH:$PATH" aces-cli --multirun \
  hydra/launcher=joblib \
  data=sharded \
  data.standard=meds \
  data.root="$DATA_DIR" \
  "data.shard=$(expand_shards $DATA_DIR)" \
  cohort_dir=$TASK_COHORT_DIR \
  cohort_name=$TASK_NAME