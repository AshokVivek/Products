FROM python:3.10

RUN apt-get update && apt-get upgrade -y
RUN apt-get install ffmpeg libsm6 libxext6 -y
RUN apt-get install poppler-utils -y
RUN apt-get install libleptonica-dev tesseract-ocr libtesseract-dev \
                    python3-pil tesseract-ocr-eng \
                    tesseract-ocr-script-latn -y

WORKDIR /code

# copy requirements.txt
COPY ./requirements.txt /code/requirements.txt

# upgrade pip
RUN pip install --upgrade pip

# installing dependencies
RUN cat requirements.txt | xargs -n 1 pip install --upgrade --force-reinstall
RUN pip install "numpy<2.0" 
RUN pip install pandas

RUN pip install clickhouse_connect

COPY ./app /code/app
COPY ./fsm_lambdas fsm_lambdas
COPY ./fsm_lambdas/library library
COPY ./fsm_lambdas/python python

# setting required environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
RUN pip install pandas==2.2.1
RUN pip install pdfplumber==0.11.0
RUN pip install fuzzywuzzy==0.18.0
RUN pip install holidays==0.46

CMD ["sh", "-c", "uvicorn app.main:app --host=0.0.0.0 --port=8000 --workers=$((2 * $(nproc) - 3)) ", "--timeout", "900"]