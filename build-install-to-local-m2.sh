#!/bin/bash

# install to local maven ~/.m2 repo
# then have your dependency on 2.6.5-SNAPSHOT version of the SDK
# this should keep your dependencies get resolved from pom files
mvn clean package -DskipTests
VERSION=2.6.5-SNAPSHOT
mvn install:install-file -Dfile=pom.xml -DpomFile=pom.xml
mvn install:install-file -Dfile=commons/target/azure-cosmosdb-commons-$VERSION.jar  -DpomFile=commons/pom.xml
mvn install:install-file -Dfile=commons-test-utils/target/azure-cosmosdb-commons-test-utils-$VERSION.jar  -DpomFile=commons-test-utils/pom.xml
mvn install:install-file -Dfile=gateway/target/azure-cosmosdb-gateway-$VERSION.jar  -DpomFile=gateway/pom.xml
mvn install:install-file -Dfile=direct-impl/target/azure-cosmosdb-direct-$VERSION.jar  -DpomFile=direct-impl/pom.xml
mvn install:install-file -Dfile=sdk/target/azure-cosmosdb-$VERSION.jar  -DpomFile=sdk/pom.xml
