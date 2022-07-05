#!/usr/bin/env bash

tester=$(find . -name "r3c_stress_hash")
redis=127.0.0.1:6379
n_circle=100000

for n_field in 10 15 20 25 30
do
  $tester $redis $n_field $n_circle
  echo
done

