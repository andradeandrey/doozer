#!/bin/sh
goinstall github.com/bmizerany/assert
make clean
make
make test
