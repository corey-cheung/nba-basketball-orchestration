#!/bin/bash

set -eufo pipefail

echo "Setting up nba-basketball-orchestration conda env"

# Config shell session to work with conda
. ~/miniconda3/etc/profile.d/conda.sh

conda deactivate
conda env remove -n nba-basketball-orchestration
conda create -yqn nba-basketball-orchestration python=3.10
conda activate nba-basketball-orchestration

echo "Finished, now spin up your new conda environment with 'conda activate nba-basketball-orchestration'"
