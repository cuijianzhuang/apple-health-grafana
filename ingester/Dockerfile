FROM python:3.13-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py .

RUN adduser -D appuser
USER appuser

ENTRYPOINT ["python3", "-u"]
CMD ["app.py"]
