# Stage 1: Define a slim, common base using Python 3.9
FROM python:3.9-slim as base

# Set the working directory
WORKDIR /app

# Prevent Python from writing .pyc files and ensure output is unbuffered
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Stage 2: Install dependencies for Python 3.9
FROM base as builder

# Copy ONLY the requirements file first to leverage caching
COPY requirements.txt .

# Install the dependencies. This layer will be cached.
RUN pip install --no-cache-dir -r requirements.txt

# Stage 3: Create the final, clean image
FROM base as final

# Copy the installed packages from the 'builder' stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Copy the application code
COPY . .

# Set the default command to run the application
CMD ["python", "app.py"]

