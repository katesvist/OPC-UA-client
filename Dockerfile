FROM python:3.12-slim

WORKDIR /service

COPY pyproject.toml README.md /service/
COPY src /service/src
COPY examples /service/examples

RUN pip install --no-cache-dir .

ENV OPC_CONFIG_FILE=/service/examples/config.edge.yaml

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
