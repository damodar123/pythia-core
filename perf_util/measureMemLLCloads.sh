#!/bin/bash

perf stat -e LLC-loads -aA -o out -v -x, --pre ./wait.start ./wait.stop
