FROM python:3.10

LABEL maintainer="Siddhant Tiwary <siddhant.tiwary@finbox.in>"

ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils && \
    apt-get install -y --no-install-recommends libgl1-mesa-glx

RUN wget https://github.com/DanBloomberg/leptonica/releases/download/1.84.0/leptonica-1.84.0.tar.gz && \
    tar -xvf leptonica-1.84.0.tar.gz && \
    cd leptonica-1.84.0 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm leptonica-1.84.0.tar.gz

RUN apt-get install -y --no-install-recommends tesseract-ocr

RUN wget https://github.com/ArtifexSoftware/ghostpdl-downloads/releases/download/gs10021/ghostscript-10.02.1.tar.gz && \
    tar -xvf ghostscript-10.02.1.tar.gz && \
    cd ghostscript-10.02.1 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm ghostscript-10.02.1.tar.gz

# copy required dependency files
COPY requirements.docker.txt requirements.txt

COPY local_wheels/category-0.0.1-py2.py3-none-any.whl local_wheels/category-0.0.1-py2.py3-none-any.whl

# install required dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

RUN pip3 install git+https://github.com/pdftables/python-pdftables-api.git

RUN pip3 install --no-cache-dir confluent-kafka==2.8.0

RUN pip3 install local_wheels/category-0.0.1-py2.py3-none-any.whl

# remove requirements and dependent files
RUN rm requirements.txt

RUN rm -rf local_wheels/

COPY subscriber.main.py subscriber.main.py

COPY context/ context/

COPY library/ library/

COPY python/ python/

CMD ["ddtrace-run", "python3", "subscriber.main.py"]