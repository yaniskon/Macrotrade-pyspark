#Set a standard Python base image (similar to gitpod/workspace-python)
FROM python:3.11-slim AS base

# Set working directory
WORKDIR /app

# Copy Python project files (pyproject.toml for Poetry)
#COPY ./pyproject.toml /app/pyproject.toml
COPY . /app
# Install Python and Poetry dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    gnupg \
    lsb-release \
    ca-certificates

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set PATH for Poetry
ENV PATH="/root/.local/bin:$PATH"

# Install project dependencies using Poetry
#RUN poetry install --no-root

# --- Java Installation ---
FROM base AS java-installer

USER root
WORKDIR /opt

# Determine architecture and download OpenJDK
RUN ARCHITECTURE=$(dpkg --print-architecture) && \
    if [ "$ARCHITECTURE" = "arm64" ] ; then ARCH="aarch64"; else ARCH="x64"; fi && \
    wget -O OpenJDK.tar.gz "https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_${ARCH}_linux_hotspot_11.0.11_9.tar.gz"

# Extract OpenJDK
RUN tar xzf OpenJDK.tar.gz

# Set Java environment variables
ENV JAVA_HOME="/opt/jdk-11.0.11+9"
ENV PATH="/opt/jdk-11.0.11+9/bin:$PATH"

# --- Final Image ---
FROM base

# Copy the installed Java from the java-installer stage
COPY --from=java-installer /opt/jdk-11.0.11+9 /opt/jdk-11.0.11+9
ENV JAVA_HOME="/opt/jdk-11.0.11+9"
ENV PATH="/opt/jdk-11.0.11+9/bin:$PATH"

# Copy the application code
#COPY . /app
# Copy the application code and the virtual environment from the previous stage
COPY --from=0 /app /app

# Set working directory
WORKDIR /app
# Switch back to a non-root user (you might need to adjust the user ID)
# Create a non-root user if it doesn't exist
#RUN adduser --disabled-password --gecos '' myuser
#USER myuser

# Explicitly install pytest using pip within the activated virtual environment
#RUN . /app/.venv/bin/activate && pip install pytest

# Set the entrypoint to run your Python application (adjust as needed)
ENTRYPOINT ["poetry", "run"] 

# Default command
CMD ["python"]
