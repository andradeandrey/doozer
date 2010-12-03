#!/bin/sh
set -e
set -x

## Prereqsuisites
apt-get install -y\
    git-core

easy_install -U\
    mercurial

## Workarea
cd /opt/

## Go
hg clone -r release https://go.googlecode.com/hg/ go
(
    cd go/src
    ./all.bash
)

## Doozer
git clone http://github.com/bmizerany/doozer
(
    cd doozer/src
    ./all.sh
)
