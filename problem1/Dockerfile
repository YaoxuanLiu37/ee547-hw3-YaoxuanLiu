FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# Files are bind-mounted by docker-compose; copying is optional
# COPY schema.sql load_data.py queries.py ./

CMD ["python", "load_data.py"]
