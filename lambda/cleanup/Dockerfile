# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

# Force wheel resolution, never source builds
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ONLY_BINARY=:all:

COPY cleanup/pyproject.toml ./
RUN pip install --target /var/task .
COPY cleanup/handler.py /var/task

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

### Set the CMD to your handler
CMD ["handler.handler"]
