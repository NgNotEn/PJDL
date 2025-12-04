# A parallel join system with dynamic load work.
The system is built based on the ADOPT. The main branch is a common version with a web interface, and you can checkout imdb branch to do benchmark of imdb.


## How to build
```bash
JAVA_FILES=$(find . -name "*.java")
javac -encoding UTF-8 -d bin -sourcepath src/ -classpath lib/\* $JAVA_FILES
```

## How to create db
```bash
cd bin
java tools.CreateDB db_name db_save_path
```

## How to do some sql
```bash
java -classpath .:../lib/* -Xmx16G -XX:+UseConcMarkSweepGC console.SkinnerCmd db_save_path
```
Then you can input sqls or execute sql file. Like:
```bash
exec create.sql
load table_name PATH_TO_CSV_FILE , NULL;
```

## How to run the program
There is a web page in src folder as the interface.
If you want to run the webserver.By the way you should change the database path in code(server.WebServer).
```bash
cd bin
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* server.WebServer
```
If you wan to run the benchmarks.
```bash
cd bin
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify database_path queries_folder_path
```

