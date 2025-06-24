# FROM python:3.12-slim

# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONUNBUFFERED 1

# WORKDIR /app

# RUN apt-get update && apt-get install -y \
#     gcc \
#     libffi-dev \
#     libssl-dev \
#     default-libmysqlclient-dev \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# COPY requirements.txt .
# RUN pip install --upgrade pip
# RUN pip install -r requirements.txt

# COPY . .

# # RUN python manage.py collectstatic --noinput

# EXPOSE 8000

# # CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

# CMD ["gunicorn", "--reload", "inn_report_b.wsgi:application", "--bind", "0.0.0.0:8000"]

# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    libssl-dev \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

# Entrypoint dinámico (se decide en docker-compose)
CMD ["gunicorn", "--reload", "inn_report_b.wsgi:application", "--bind", "0.0.0.0:8000"]

