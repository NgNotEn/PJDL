cd bin
# timestamp=$(date +"%Y%m%d_%H%M%S")
# java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../../ADOPT-tpch/jcch ../../ADOPT-tpch/tpch/queries >../res/jcch_benchmark-64-threads.txt 2>&1
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../../ADOPT-tpch/tpch ../../ADOPT-tpch/tpch/queries >../res/tpch_benchmark-64-threads.txt 2>&1

# java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../../ADOPT-tpch/ego-facebook ../../ADOPT-tpch/ego-facebook/queries/clique >../res/ego-facebook_benchmark-clique.txt 2>&1
# java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Xmx256G -Xms256G -classpath .:../lib/* benchmark.BenchAndVerify ../../ADOPT-tpch/ego-twitter /opt/data/private/ADOPT-tpch/ego-twitter/graph_selectivity_query/twitter_selectivity_3clique/sql  >../res/ego-twitter_benchmark-3clique.txt 2>&1


