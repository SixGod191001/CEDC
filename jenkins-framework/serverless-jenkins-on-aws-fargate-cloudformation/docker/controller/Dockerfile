FROM jenkins/jenkins:lts-jdk11
# Use Configuration as Code to setup the jenkins
# https://www.jenkins.io/doc/book/managing/casc/
ENV CASC_JENKINS_CONFIG /usr/share/jenkins/ref/jenkins.yaml
COPY files/jenkins.yaml /usr/share/jenkins/ref/jenkins.yaml
RUN echo 2.0 > /usr/share/jenkins/ref/jenkins.install.UpgradeWizard.state
COPY files/initialConfig.groovy /usr/share/jenkins/ref/init.groovy.d/initialConfig.groovy
COPY files/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli -f /usr/share/jenkins/ref/plugins.txt
# Install AWSCLIV2
USER root
RUN apt update && apt install -y curl unzip groff
    
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm awscliv2.zip && \
    rm -rf aws


