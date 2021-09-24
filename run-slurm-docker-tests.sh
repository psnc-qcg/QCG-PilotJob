#!/bin/bash

test_cmdline="/srv/src/pytest-cov.sh $* src/"
echo "test cmdline: $test_cmdline"

docker exec -it slurmctld sbatch -N 2 --ntasks-per-node 2 --overcommit --oversubscribe -o /srv/src/slurm-tests-out.txt -D /srv --exclusive --wait --wrap="$test_cmdline"
