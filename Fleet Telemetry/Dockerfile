FROM python:3.9

# Cài đặt thư viện kafka-python
RUN pip install kafka-python

# Sao chép script vào container
COPY ./kafka-scripts/create_topics.py /create_topics.py

# Thực thi script
CMD ["python", "/create_topics.py"]
