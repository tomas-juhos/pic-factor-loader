FROM python:3.10.5-slim

LABEL maintainer="Tomas Juhos"
USER root

WORKDIR /project

COPY requirements.txt .

# install dependencies
RUN apt-get update
RUN apt-get install -y make \
                       gcc \
                       openssl \
                       libssl-dev \
                       zlib1g-dev \
    && python -m pip install --no-cache-dir --upgrade pip \
    && pip3 install -r requirements.txt \
    && rm -rf ~/.cache \
    && find . -type d -name __pycache__ -exec rm -r {} \+ \
    && find . -type d -name .pyc -exec rm -r {} \+ \
    && find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$)" | xargs rm -rf \
    && apt-get clean \
    && apt-get autoremove -y \
    && apt-get install dumb-init

COPY src/ .
COPY pyproject.toml .
COPY README.md .
RUN pip3 install .

EXPOSE 9000

CMD ["python", "-m", "factor_loader"]
