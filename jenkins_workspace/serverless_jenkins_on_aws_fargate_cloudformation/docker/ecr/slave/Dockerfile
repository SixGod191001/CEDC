FROM jenkins/inbound-agent
# Install AWSCLIV2
USER root
RUN apt update && apt install -y curl unzip python3 python3-pip groff && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip  && \
    ./aws/install && \
    rm awscliv2.zip && \
    rm -rf aws && \
    ln -s /usr/bin/python3.9 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/archives/*
USER jenkins




