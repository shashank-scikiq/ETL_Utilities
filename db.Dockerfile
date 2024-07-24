FROM python:3.12.4-slim

WORKDIR /app/init_db

COPY Init_DB/ /app/init_db/
COPY Init_DB/Final_DB_Scripts /app/init_db/Final_DB_Scripts/
COPY Init_DB/Python_Scripts /app/init_db/Python_Scripts/
COPY Init_DB/Final_DB_Scripts/* /app/init_db/Python_Scripts/

COPY requirements.txt /app/init_db/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "Python_Scripts/ETL_Loader.py"]