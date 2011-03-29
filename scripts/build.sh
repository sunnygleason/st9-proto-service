#!/bin/sh

mvn clean
mvn package dependency:copy-dependencies

