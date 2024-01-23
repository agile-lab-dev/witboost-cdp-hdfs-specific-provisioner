
FROM eclipse-temurin:8u392-b08-jre-jammy

RUN apt update && apt install -y protobuf-compiler netcat && rm -rf /var/lib/apt/lists/*

WORKDIR /work

ADD https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz /work/hadoop-3.3.6.tar.gz
RUN tar xzf hadoop-3.3.6.tar.gz && rm -f hadoop-3.3.6.tar.gz

ADD https://repo1.maven.org/maven2/org/mockito/mockito-core/3.12.4/mockito-core-3.12.4.jar /work/hadoop-3.3.6/share/hadoop/common/lib/mockito-core-3.12.4.jar

WORKDIR /work/hadoop-3.3.6

ENV HADOOP_HOME=/work/hadoop-3.3.6
ENV PATH="$PATH:/work/hadoop-3.3.6/bin"

EXPOSE 9000 9001

CMD ["mapred", "minicluster", "-format", "-nnport", "9000", "-nnhttpport", "9001", "-nomr"]
