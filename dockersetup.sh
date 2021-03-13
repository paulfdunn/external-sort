#!/bin/bash
#
# If you are running as non-root user, run this script to
# prevent 'Got permission denied while trying to connect to the Docker daemon socket'
# errors. 
# See link for more details: https://docs.docker.com/engine/install/linux-postinstall/
#
set -x
sudo groupadd docker
sudo usermod -aG docker ${USER}
# This is required on Pixelbook, but may not be otherwise required
sudo chmod 666 /var/run/docker.sock