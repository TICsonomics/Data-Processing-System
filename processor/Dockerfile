FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY processor.py processor.py
CMD ["python", "-u", "processor.py"]

