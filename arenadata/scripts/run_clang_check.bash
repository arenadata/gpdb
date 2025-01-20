#!/bin/bash

set -xe -o pipefail

make -s -j`nproc`
