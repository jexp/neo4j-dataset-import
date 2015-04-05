mvn clean compile exec:java -Dexec.mainClass="friendster.ImportFriendster" -Dexec.classpathScope=test -Dexec.args="${1-friendster.db} ${2-data}"
