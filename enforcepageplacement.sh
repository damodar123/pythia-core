#!/bin/bash
# Enforces page placement on NUMA nodes.
LD_LIBRARY_PATH=dist/lib drivers/enforcepageplacement_move /dev/shm/\*numa0
LD_LIBRARY_PATH=dist/lib drivers/enforcepageplacement_move /dev/shm/\*numa1
LD_LIBRARY_PATH=dist/lib drivers/enforcepageplacement_move /dev/shm/\*numa2
LD_LIBRARY_PATH=dist/lib drivers/enforcepageplacement_move /dev/shm/\*numa3
