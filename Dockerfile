# Use the official Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Copy the frontend and backend code into the container
COPY . .

# Expose the port on which your application will run
EXPOSE 27010

# Set the entry point command
CMD ["python", "Migration_Project/home.py"]
