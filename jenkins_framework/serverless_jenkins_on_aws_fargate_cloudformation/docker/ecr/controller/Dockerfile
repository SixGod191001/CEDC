FROM jenkins/jenkins:lts-jdk11
USER root
ENV CASC_JENKINS_CONFIG /usr/share/jenkins/ref/jenkins.yaml
COPY files/jenkins.yaml /usr/share/jenkins/ref/jenkins.yaml
COPY files/initialConfig.groovy /usr/share/jenkins/ref/init.groovy.d/initialConfig.groovy
COPY files/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN echo 2.0 > /usr/share/jenkins/ref/jenkins.install.UpgradeWizard.state && \
    jenkins-plugin-cli -f /usr/share/jenkins/ref/plugins.txt && \
    apt update && apt install -y curl unzip python3 python3-pip groff && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip  && \
    ./aws/install && \
    rm awscliv2.zip && \
    rm -rf aws && \
    ln -s /usr/bin/python3.9 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/archives/*
USER jenkins
