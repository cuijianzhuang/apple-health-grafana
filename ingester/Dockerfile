FROM python:3.14-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py .
COPY static ./static

RUN adduser -D appuser
USER appuser

ENTRYPOINT ["python3", "-u"]
CMD ["app.py"]
