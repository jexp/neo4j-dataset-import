mvn clean compile exec:java -Dexec.mainClass="friendster.ImportFriendster" -Dexec.args="${1-friendster.db} ${2-data}"
