FROM python:3.8-slim
RUN useradd --create-home --shell /bin/bash app_user
WORKDIR /home/app_user
COPY requirements.txt ./
RUN pip install -r requirements.txt
USER app_user
COPY . .
CMD ["bash"]
#HEALTHCHECK CMD curl -fsSL "http://127.0.0.1:9200/_cat/health?h=status" | grep -E '^green'
#CMD [ "python ETL_mechanism.py" ]