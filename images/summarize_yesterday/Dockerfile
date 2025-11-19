FROM python:3.13-slim

# Set the working directory
WORKDIR /app

# Install tzdata for timezone support
RUN apt-get update && apt-get install -y --no-install-recommends tzdata
ENV TZ=America/Chicago

# Copy the requirements file into the container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY summarize summarize
COPY scripts scripts
COPY cli cli
COPY data data
COPY util util

# Make scripts executable
RUN chmod +x scripts/*

ENTRYPOINT ["bash"]
CMD ["scripts/summarize_yesterday.sh"]
