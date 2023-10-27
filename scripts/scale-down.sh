#!/bin/bash

# ARRAY=( "cow:moo"
#         "dinosaur:roar"
#         "bird:chirp"
#         "bash:rock" )

# for animal in "${ARRAY[@]}" ; do
#     printf "${animal%%:*} likes to ${animal##*:}\n"
# done

APPS=(
    "test-airflow-dagprocessor"
    "test-airflow-flower"
    "test-airflow-scheduler"
    "test-airflow-triggerer"
    "test-airflow-webserver"
    "test-airflow-worker"
)

# SERVICES=(
# )

for app in "${APPS[@]}" ; do
    cf scale "${app%%:*}" -i 0
done
