#!/usr/bin/env sh
set -e
if [ -f "./gradle/wrapper/gradle-wrapper.jar" ]; then
  exec java -jar ./gradle/wrapper/gradle-wrapper.jar "$@"
fi
if command -v gradle >/dev/null 2>&1; then
  exec gradle "$@"
fi
echo "Gradle wrapper jar is not bundled in this generated archive. Install Gradle 9.x or run 'gradle wrapper --gradle-version 9.5.0' once, then use ./gradlew." >&2
exit 1
