mvn clean compile exec:java -Dexec.mainClass="friendster.GenerateFriendster" -Dexec.classpathScope=test -Dexec.args="${1-friendster.db} ${2-data}"
