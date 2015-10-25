@echo off
@rem Script to check pre-requisites and build Apache Apex

setlocal

@rem check for git

call :check_cmd git
if %errorlevel% neq 0 exit /b %errorlevel%

@rem check for maven

call :check_cmd mvn
if %errorlevel% neq 0 exit /b %errorlevel%

@rem check for javac

call :check_cmd javac
if %errorlevel% neq 0 exit /b %errorlevel%

@rem check for java

call :check_cmd java
if %errorlevel% neq 0 exit /b %errorlevel%

@rem check java version

call :check_java
if %errorlevel% neq 0 exit /b %errorlevel%

@rem increase maven memory options
set MAVEN_OPTS=-Xms256m -Xmx1024m -XX:PermSize=256m

@rem clone apex-core and build it
call :build_core
if %errorlevel% neq 0 exit /b %errorlevel%

@rem clone apex-malhar and build it
call :build_malhar
if %errorlevel% neq 0 exit /b %errorlevel%

@rem wrapup
echo looks good
exit /b 0

@rem quasi-functions

@rem check if command in %1 exists
:check_cmd
where %1 >nul 2>nul
if %errorlevel% neq 0 (
  echo %1 command not found
  exit /b %errorlevel%
)
exit /b 0

@rem check java version
:check_java
for /f tokens^=2-5^ delims^=.-_^" %%j in ('java -fullversion 2^>^&1') do @set "jver=%%j%%k%%l%%m"
if %jver% lss 17079 (
  echo java version too old
)
exit /b 0

:build_core
echo building apex-core
call git clone https://github.com/apache/incubator-apex-core.git
cd incubator-apex-core
call git checkout release-3.1
call mvn clean install -DskipTests
cd ..
exit /b 0

:build_malhar
echo building apex-malhar
call git clone https://github.com/apache/incubator-apex-malhar.git
cd incubator-apex-malhar
call git checkout release-3.1
call mvn clean install -DskipTests
cd ..
exit /b 0

endlocal
