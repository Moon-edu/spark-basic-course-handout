#!/bin/bash

# Update package lists
sudo apt update -y

# Upgrade installed packages
sudo apt upgrade -y

# Install Python 3
sudo apt install -y python3

# Create a symbolic link for 'python' command to point to 'python3'
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Install git-lfs
sudo apt install -y git-lfs

# Install libpq-dev
sudo apt install -y libpq-dev

# Install OpenJDK 8
sudo apt install -y openjdk-8-jdk

# Install zip
sudo apt install -y zip

# Install xz
sudo apt install -y xz-utils

# Repo
git clone https://github.com/Moon-edu/spark-basic-course-handout.git
cd spark-basic-course-handout
git lfs install
git pull

cd ~
git clone https://github.com/Moon-edu/spark-basic-course-hw.git
cd spark-basic-course-hw
git lfs install
git pull
