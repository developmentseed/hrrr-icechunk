# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

# Force wheel resolution, never source builds
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ONLY_BINARY=:all:

# Install build dependencies, install packages, then clean up in one layer
RUN dnf -y install git && \
    dnf clean all

COPY append/pyproject.toml ./
RUN pip install --target /var/task .
COPY hrrr.py append/handler.py /var/task

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

### Set the CMD to your handler
CMD ["handler.handler"]
