# 注释

FROM openjdk:17-jdk-slim      #基础镜像，相当于操作系统

WORKDIR /app                   #设置工作目录

COPY target/Server-1.0-SNAPSHOT.jar app.jar  #把文件复制进镜像

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092   #设置环境变量

EXPOSE 8080                    #声明端口（不是真正开放，只是文档说明）

ENTRYPOINT ["java", "-jar", "app.jar"]       #容器启动时执行的命令