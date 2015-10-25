@echo off
@rem Script for creating a new application

setlocal

mvn archetype:generate ^
-DarchetypeRepository=https://www.datatorrent.com/maven/content/repositories/releases ^
  -DarchetypeGroupId=com.datatorrent ^
  -DarchetypeArtifactId=apex-app-archetype ^
  -DarchetypeVersion=3.1.1 ^
  -DgroupId=com.example ^
  -Dpackage=com.example.myapexapp ^
  -DartifactId=myapexapp ^
  -Dversion=1.0-SNAPSHOT

endlocal
