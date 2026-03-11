FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV WEIR_LISTEN_HOST=0.0.0.0
ENV WEIR_LISTEN_PORT=5433
CMD ["python", "main.py"]
