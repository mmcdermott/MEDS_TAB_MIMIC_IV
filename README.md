# Using MEDS-TAB on MIMIC-IV

Scripts, configs, and notebooks for running MEDS-TAB on MIMIC-IV tasks.

The MIMIC-IV dataset is derived via [the meds polars repo](https://github.com/mmcdermott/MEDS_polars_functions/blob/f979ea46416b9b8f307eedac9efaf31146b746d8/MIMIC-IV_Example/joint_script.sh)

You should define a `.env` file with the following variables:

```bash
MIMICIV_MEDS_DIR=[PATH TO MEDS DATASET ROOT]
MED_TABS_MIMIC_IV_DIR=[PATH TO THIS REPO ROOT]
```

You should clone [this repo](https://github.com/mmcdermott/MEDS_Tabular_AutoML/) and run `pip install .` from
the branch "config".

Supported tasks:
  * `mortality/in_hospital/first_48h`
  * `mortality/in_hospital/first_24h`
  * `mortality/in_icu/first_48h`
  * `mortality/in_icu/first_24h`
  * `mortality/post_hospital_discharge/30d`
  * `readmission/30d`
