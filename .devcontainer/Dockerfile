ARG VARIANT="3.11"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ARG NODE_VERSION="none"
RUN if ["${NODE_VERSION}}" != "none" ]; then su vscode -c "unmask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION}} 2>&1"; fi

# Install additional OS-level dependencies for Python packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        libldap2-dev \
        libsasl2-dev

# Remove default utils that could be mistakenly used instead of poetry-controlled equivalents.
ARG DEFAULT_UTILS_TO_UNINSTALL="\
    pylint \
    flake8 \
    autopep8 \
    black \
    yapf \
    mypy \
    pydocstyle \
    pycodestyle \
    bandit \
"
RUN echo "${DEFAULT_UTILS_TO_UNINSTALL}" | xargs -n 1 pipx uninstall

ARG DEFAULT_UTILS_TO_INSTALL="\
    pre-commit \
    black \
    isort \
"

RUN echo "${DEFAULT_UTILS_TO_INSTALL}" | xargs -n 1 pipx install

# Install Poetry for vscode user
RUN su vscode -c "curl -sSL https://install.python-poetry.org | python3 - --version 1.8.1 2>&1"
RUN su vscode -c "echo `source ~/.poetry/env` >> ~/.zshrc 2>&1"
RUN su vscode -c "echo `source ~/.poetry/env` >> ~/.bashrc 2>&1"

# Spark + Hadoop
ENV SPARK_VERSION=3.1.1
ENV HADOOP_VERSION=3.2

# Installing Spark and Hadoop
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz 
    
# Installing Java
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV DEBUG=true
ENV SPARK_HOME=/spark/
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
