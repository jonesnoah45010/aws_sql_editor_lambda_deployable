# Dockerfile
FROM public.ecr.aws/lambda/python:3.12

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ALL app code
COPY . .

# Point Lambda to the handler in lambda_handler.py
CMD ["lambda_handler.handler"]
