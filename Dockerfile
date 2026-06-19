FROM apache/airflow:2.9.0

# Install pipeline dependencies as the airflow user (no root needed for pip)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-download DuckDB httpfs extension so transforms work without internet at runtime.
# The || true ensures the build succeeds even if the extension CDN is temporarily unreachable;
# DuckDB will retry the download the first time a transform runs.
RUN python -c "\
import duckdb; \
con = duckdb.connect(); \
con.execute('INSTALL httpfs'); \
con.close()" || true
