@echo off
@rem Script for creating a new application

setlocal

mvn -B archetype:generate ^
  -DarchetypeGroupId=org.apache.apex ^
  -DarchetypeArtifactId=apex-app-archetype ^
  -DarchetypeVersion=3.3.0-incubating ^
  -DgroupId=com.example ^
  -Dpackage=com.example.myapexapp ^
  -DartifactId=myapexapp ^
  -Dversion=1.0-SNAPSHOT

endlocal
