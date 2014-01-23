all:
	javac -classpath PageRank/lib/hadoop-core-1.0.3.jar PageRank/*.java
	jar -cvf PageRank.jar PageRank/*.class

clean:
	hadoop fs -rmr /test/
