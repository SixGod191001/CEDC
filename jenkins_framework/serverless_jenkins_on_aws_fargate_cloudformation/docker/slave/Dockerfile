FROM jenkins/inbound-agent
# Install AWSCLIV2
USER root
RUN apt update && apt install -y curl unzip groff
    
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm awscliv2.zip && \
    rm -rf aws



