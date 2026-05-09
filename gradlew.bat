@echo off
if exist gradle\wrapper\gradle-wrapper.jar (
  java -jar gradle\wrapper\gradle-wrapper.jar %*
  exit /b %ERRORLEVEL%
)
where gradle >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  gradle %*
  exit /b %ERRORLEVEL%
)
echo Gradle wrapper jar is not bundled in this generated archive. Install Gradle 9.x or run "gradle wrapper --gradle-version 9.5.0" once, then use gradlew.bat.
exit /b 1
