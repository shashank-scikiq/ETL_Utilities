FROM python:3.11.9-slim

WORKDIR /app
WORKDIR /app/init_db

COPY Init_DB/ /app/init_db/
COPY Init_DB/Final_DB_Scripts /app/init_db/Final_DB_Scripts/
COPY Init_DB/Python_Scripts /app/init_db/Python_Scripts/
COPY Init_DB/Final_DB_Scripts/* /app/init_db/Python_Scripts/

COPY /Init_DB/requirements.txt /app/init_db/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "Python_Scripts/ETL_Loader.py"]