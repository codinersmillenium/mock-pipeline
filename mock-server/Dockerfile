FROM python:3.10-slim

LABEL maintainer="Mock Server"
LABEL description="Flask mock server — serves customer data from JSON"

WORKDIR /app

# Install dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]