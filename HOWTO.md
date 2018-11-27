# How To Build:

```bash
git clone https://github.com/Azure/azure-cosmosdb-java.git
cd azure-cosmosdb-java
git checkout leak-tmp-fix

mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \
                   -DgroupId=com.microsoft.azure -DartifactId=azure-cosmosdb-direct -Dversion=2.2.2 \
                   -Dtransitive=false
zip -d  ~/.m2/repository/com/microsoft/azure/azure-cosmosdb-direct/2.2.2/azure-cosmosdb-direct-2.2.2.jar \
                    META-INF/MSFTSIG.RSA META-INF/MSFTSIG.SF

mvn clean package
```
