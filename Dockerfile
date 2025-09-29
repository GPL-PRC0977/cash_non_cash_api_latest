FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8080 for Cloud Run
EXPOSE 8080

# Set environment variable for Flask app
ENV FLASK_APP=app.py

# Use gunicorn to run the Flask app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
