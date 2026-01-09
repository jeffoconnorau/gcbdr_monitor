# ---- Builder Stage ----
FROM python:3.11-slim as builder

WORKDIR /app

# Create a non-root user
RUN useradd -m nonroot
USER nonroot
WORKDIR /home/nonroot/app

# Create a virtual environment
RUN python -m venv /home/nonroot/venv
ENV PATH="/home/nonroot/venv/bin:$PATH"

# Copy the requirements file and install dependencies
COPY --chown=nonroot:nonroot requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Final Stage ----
FROM python:3.11-slim

# Create a non-root user
RUN useradd -m nonroot
USER nonroot
WORKDIR /home/nonroot/app

# Copy the virtual environment from the builder stage
COPY --from=builder /home/nonroot/venv /home/nonroot/venv
ENV PATH="/home/nonroot/venv/bin:$PATH"

# Copy the application code and credentials
COPY --chown=nonroot:nonroot src/ ./src/

# Set python path
ENV PYTHONPATH=/home/nonroot/app

# Run the application
CMD ["python", "src/main.py"]
