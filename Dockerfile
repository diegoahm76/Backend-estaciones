# base image  
FROM python:3.11.0   
# setup environment variable  
ENV DockerHOME=/home/app/backend-estaciones  

# set work directory  
RUN mkdir -p $DockerHOME  

# where your code lives  
WORKDIR $DockerHOME  

# set environment variables  
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1  

# install dependencies  
RUN pip install --upgrade pip
RUN apt-get update
RUN apt-get install -y cron

# copy whole project to your docker home directory. 
COPY . $DockerHOME  
# run this command to install all dependencies  
RUN pip install -r requirements.txt  
# port where the Django app runs  
EXPOSE 8000  
# start server
CMD service cron start && python manage.py crontab add && python manage.py crontab show && python manage.py runserver