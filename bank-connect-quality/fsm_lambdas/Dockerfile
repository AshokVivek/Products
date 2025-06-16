FROM public.ecr.aws/lambda/python:3.10-arm64

RUN yum -y update && \
    yum -y install wget && \
    yum install -y tar && \
    yum clean all && \
    yum install git -y && \
    yum install mesa-libGL -y && \
    yum install poppler-utils -y

RUN yum -y install autoconf automake libtool autoconf-archive pkg-config make libjpeg-devel libpng-devel libtiff-devel zlib-devel
RUN yum group install -y "Development Tools"

RUN wget https://github.com/DanBloomberg/leptonica/releases/download/1.84.0/leptonica-1.84.0.tar.gz && \
    tar -xvf leptonica-1.84.0.tar.gz && \
    cd leptonica-1.84.0 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm leptonica-1.84.0.tar.gz

RUN wget https://github.com/tesseract-ocr/tesseract/archive/refs/tags/5.3.3.tar.gz && \
    tar -xvf 5.3.3.tar.gz && \
    cd tesseract-5.3.3 && \
    ./autogen.sh && \
    export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig/ && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm 5.3.3.tar.gz && \
    wget https://github.com/tesseract-ocr/tessdata/raw/main/eng.traineddata && \
    mv *.traineddata /usr/local/share/tessdata

RUN wget https://github.com/ArtifexSoftware/ghostpdl-downloads/releases/download/gs10021/ghostscript-10.02.1.tar.gz && \
    tar -xvf ghostscript-10.02.1.tar.gz && \
    cd ghostscript-10.02.1 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm ghostscript-10.02.1.tar.gz

RUN rm -rf /var/cache/yum

COPY local_wheels/ ${LAMBDA_TASK_ROOT}/local_wheels

COPY requirements.docker.txt ./requirements.txt

RUN pip3 install -r ./requirements.txt

RUN pip3 install git+https://github.com/pdftables/python-pdftables-api.git

RUN pip3 install ${LAMBDA_TASK_ROOT}/local_wheels/category-0.0.1-py2.py3-none-any.whl

COPY score/ ${LAMBDA_TASK_ROOT}

COPY library/ ${LAMBDA_TASK_ROOT}/library

COPY python/ ${LAMBDA_TASK_ROOT}/python/

COPY python/extraction_handler.py ${LAMBDA_TASK_ROOT}

COPY python/cc_utils_ocr.py ${LAMBDA_TASK_ROOT}

COPY python/identity_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/handlers.py ${LAMBDA_TASK_ROOT}

COPY python/update_state_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/metadata_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/quality_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/finvu_aa_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/perfios_integration_iifl_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/perfios_pdf_upload_to_bc.py ${LAMBDA_TASK_ROOT}

COPY python/bc_p_mapping.py ${LAMBDA_TASK_ROOT}

COPY python/scanned_pdf.py ${LAMBDA_TASK_ROOT}

COPY python/xml_report_handler.py ${LAMBDA_TASK_ROOT}

COPY python/cache_subscribed_data_handler.py ${LAMBDA_TASK_ROOT}

COPY python/webhook_handlers.py ${LAMBDA_TASK_ROOT}

COPY python/dms_push_handler.py ${LAMBDA_TASK_ROOT}

CMD ["cc_utils_ocr.cc_identity_handler"]