set -eu
libraries=$(find /app/lib -name '*.jar' -print | sort | paste -sd, -)
if [ "${1:-}" = test ]; then
    shift
    export RETAILPULSE_SPARK_TESTS=true
    exec /opt/spark/bin/spark-submit --jars "$libraries,/app/application.jar,/app/tests.jar" --class org.junit.platform.console.ConsoleLauncher /app/test-console.jar execute --select-package com.retailpulse.offline --fail-if-no-tests --reports-dir /data/test-reports "$@"
fi
exec /opt/spark/bin/spark-submit --jars "$libraries" --class com.retailpulse.offline.OfflineMain /app/application.jar "$@"
