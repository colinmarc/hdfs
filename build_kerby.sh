#!/usr/bin/env bash

git clone https://github.com/apache/directory-kerby.git $HOME/kerby-build
cd $HOME/kerby-build
git checkout kerby-all-1.1.0
mvn package -Pdist -DskipTests