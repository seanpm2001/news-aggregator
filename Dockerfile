FROM public.ecr.aws/docker/library/python:3.9-slim-bullseye

RUN mkdir -p app
WORKDIR /app

RUN apt update
RUN apt install ffmpeg libsm6 libxext6  -y

ENV PYTHONPATH $PYTHONPATH:/app

ENV REQUIREMENTS_FILE=requirements.txt

COPY ./${REQUIREMENTS_FILE} ./requirements.txt
RUN HTTP_PROXY= HTTPS_PROXY= pip install -r requirements.txt

COPY . /app/

ENTRYPOINT ["app/bin/start.sh"]
