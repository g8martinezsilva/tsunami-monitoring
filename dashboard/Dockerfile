# Image of the two Plotly Dash dashboards (Workshop 4). The code and the Gold layer are mounted by
# compose.yaml, so the image only holds the Python packages.
FROM python:3.12-slim

COPY dashboard/requirements.txt /tmp/requirements-dashboard.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dashboard.txt

WORKDIR /app
ENV PYTHONUNBUFFERED=1 DASH_HOST=0.0.0.0
