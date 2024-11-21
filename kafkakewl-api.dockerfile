FROM eclipse-temurin:8-alpine

ENV SBT_VERSION 1.2.8
ENV SBT_HOME /usr/local/sbt
ENV PATH=${PATH}:${SBT_HOME}/bin
ENV SBT_JAR https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz

# installing some tools and sbt
RUN apk --update add bash wget curl tar git libgcc \
    && wget ${SBT_JAR} -O sbt-$SBT_VERSION.tgz -o /dev/null \
    && tar -xf sbt-$SBT_VERSION.tgz -C /usr/local \
    && echo -ne "- with sbt sbt-$SBT_VERSION\n" >> /root/.built \
    && rm sbt-$SBT_VERSION.tgz \
    && sbt about \
    && apk del wget tar \
    && rm -rf /var/cache/apk/*

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN sbt kewl-api/stage

CMD ["kewl-api/target/universal/stage/bin/kewl-api"]
