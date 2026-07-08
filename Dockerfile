# Streamlit dashboard image. Installs ONLY the slim dashboard runtime
# (requirements-dashboard.txt) — not the full dbt/GE/pytest toolchain — so the
# image stays light. A .dockerignore keeps the build context lean too.
FROM python:3.11-slim

WORKDIR /app

COPY requirements-dashboard.txt .
RUN pip install --no-cache-dir -r requirements-dashboard.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "bitcoin_pipeline/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
