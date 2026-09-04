#!/bin/bash

# from the root directory
make 
make tools
podman build -t pgetcd -f contrib/pgetcd/build/Dockerfile bin