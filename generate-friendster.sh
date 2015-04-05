mvn clean test-compile exec:java -Dexec.mainClass="friendster.GenerateFriendster" -Dexec.classpathScope=test -Dexec.args="${1-data} ${2-10} ${3-10000}"
