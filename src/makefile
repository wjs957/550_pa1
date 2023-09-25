default: all

all:
	javac -d  ../out/production/550_pa1/  *.java
	cp test_config.properties ../out/production/550_pa1/

clean:
	$(RM)  ../out/production/550_pa1/*.*

start_indexserver:
	 java -classpath ../out/production/550_pa1 IndexServer

start_peer:
	java -classpath ../out/production/550_pa1 PeerServer

run_evaluation:
	java -classpath ../out/production/550_pa1 TestClient
