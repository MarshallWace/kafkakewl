FROM eclipse-temurin:8-alpine

ENV SBT_VERSION 1.2.8
ENV SBT_HOME /usr/local/sbt
ENV PATH=${PATH}:${SBT_HOME}/bin
ENV SBT_JAR https://piccolo.link/sbt-$SBT_VERSION.tgz

# installing some tools and sbt
RUN apk --update add bash wget curl tar git
RUN wget ${SBT_JAR} -O sbt-$SBT_VERSION.tgz -o /dev/null
RUN tar -xf sbt-$SBT_VERSION.tgz -C /usr/local
RUN echo -ne "- with sbt sbt-$SBT_VERSION\n" >> /root/.built
RUN rm sbt-$SBT_VERSION.tgz
RUN sbt about
RUN apk del wget tar
RUN rm -rf /var/cache/apk/*

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN sbt kewl-api-metrics/stage

CMD ["kewl-api-metrics/target/universal/stage/bin/kewl-api-metrics"]
