
FROM python:3.12-slim

WORKDIR /app

COPY metadata_service/requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY metadata_service/ /app/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
