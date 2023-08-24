#!/bin/bash

export CFLAGS="-I $(brew --prefix graphviz)/include"
export LDFLAGS="-L $(brew --prefix graphviz)/lib"

# Call the actual Poetry command with any provided arguments
poetry install