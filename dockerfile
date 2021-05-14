FROM python:3.9.5-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY model.py main.py config.py config.json ./
ENV AM_I_IN_DOCKER Yes
CMD ["python3", "main.py"]