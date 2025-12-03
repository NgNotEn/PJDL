cd bin

# java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../DB_DATA/JOB /opt/data/private/AVPJoin-R/DB_DATA/job/queries
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../DB_DATA/JOB ../../ADOPT-tpch/JOB/queries > ../res/job_benchmark.txt
# java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../DB_DATA/TEST1 /opt/data/private/AVPJoin-R/DB_DATA/test_data_1/query
