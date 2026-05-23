FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Vytvoření složek pro volumes předem, aby se předešlo problémům s právy
RUN mkdir -p /app/data /app/public/graphs

# Uvicorn zajistí běh FastAPI serveru
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
