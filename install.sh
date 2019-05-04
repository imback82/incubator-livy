rm /usr/hdp/current/livy2-server/rsc-jars/livy-api-0.5.0.3.0.2.1-8.jar
cp ./rsc/target/jars/livy-api-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/rsc-jars/

rm /usr/hdp/current/livy2-server/rsc-jars/livy-rsc-0.5.0.3.0.2.1-8.jar
cp ./rsc/target/jars/livy-rsc-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/rsc-jars/

rm /usr/hdp/current/livy2-server/repl_2.10-jars/livy-core_2.10-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-core_2.11-0.5.1-incubating-SNAPSHOT.jar 

rm /usr/hdp/current/livy2-server/repl_2.10-jars/livy-repl_2.10-0.5.0.3.0.2.1-8.jar
cp ./repl/scala-2.10/target/jars/livy-repl_2.10-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/repl_2.10-jars/

rm /usr/hdp/current/livy2-server/repl_2.11-jars/livy-core_2.11-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-core_2.11-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/repl_2.11-jars/

rm /usr/hdp/current/livy2-server/repl_2.11-jars/livy-repl_2.11-0.5.0.3.0.2.1-8.jar
cp ./repl/scala-2.11/target/jars/livy-repl_2.11-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/repl_2.11-jars/

rm /usr/hdp/current/livy2-server/jars/livy-api-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-api-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/jars/

rm /usr/hdp/current/livy2-server/jars/livy-core_2.11-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-core_2.11-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/jars/

rm /usr/hdp/current/livy2-server/jars/livy-client-common-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-client-common-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/jars/

rm /usr/hdp/current/livy2-server/jars/livy-rsc-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-rsc-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/jars/

rm /usr/hdp/current/livy2-server/jars/livy-server-0.5.0.3.0.2.1-8.jar
cp ./server/target/jars/livy-server-0.5.1-incubating-SNAPSHOT.jar /usr/hdp/current/livy2-server/jars/

