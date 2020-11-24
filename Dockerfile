FROM amazonlinux:latest

RUN yum update -y

RUN amazon-linux-extras enable corretto8
RUN yum install java-1.8.0-amazon-corretto-devel -y

RUN mkdir -p fixengineonaws/libs

RUN mkdir -p fixengineonaws/config

COPY build/libs/fixengineonaws.jar /fixengineonaws/

COPY lib/*.* /fixengineonaws/libs/

COPY src/main/resources/config/server.cfg /fixengineonaws/config

COPY src/main/resources/config/client.cfg /fixengineonaws/config

WORKDIR /fixengineonaws

# CMD java -jar ./libs/fixengineonaws.jar ./config/server.cfg

CMD java -cp /fixengineonaws/fixengineonaws.jar:/fixengineonaws/libs/* com.amazonaws.fixengineonaws.FixEngine ./config/server.cfg


