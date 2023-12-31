from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import List
import csv

DATABASE_URL = "sqlite:///./orders.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String)
    status = Column(String)
    created_at = Column(String)
    user_id = Column(String)
    yandex_order_id = Column(String)
    app_type = Column(String)
    tariff = Column(String)
    price = Column(Float)
    fb_token = Column(String)
    driver_id = Column(String)
    is_share_trip = Column(String)
    passenger_count = Column(Integer)
    alem_order_id = Column(String)
    bonus_count = Column(Integer)
    admin_login = Column(String)
    pre_order_date = Column(String)
    platform = Column(String)
    bonus_for_order = Column(String)

Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def read_csv_file(file_path: str) -> List[Order]:
    orders = []
    with open(file_path, newline="") as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            order = Order(
                order_id=row["order_id"],
                status=row["status"],
                created_at=row["created_at"],
                user_id=row["user_id"],
                yandex_order_id=row["yandex_order_id"],
                app_type=row["app_type"],
                tariff=row["tariff"],
                price=float(row["price"]),
                fb_token=row["fb_token"],
                driver_id=row["driver_id"],
                is_share_trip=row["is_share_trip"],
                passenger_count=int(row["passenger_count"]),
                alem_order_id=row["alem_order_id"],
                bonus_count=int(row["bonus_count"]),
                admin_login=row["admin_login"],
                pre_order_date=row["pre_order_date"],
                platform=row["platform"],
                bonus_for_order=float(row["bonus_for_order"]),
            )
            orders.append(order)
    return orders

def calculate_order_stats(orders):
    total_orders = len(orders)
    total_amount = sum(order.price for order in orders)
    return {"total_orders": total_orders, "total_amount": total_amount}

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        contents = await file.read()
        decoded_contents = contents.decode("utf-8")
        csv_reader = csv.DictReader(decoded_contents.splitlines())
        orders_data = [Order(**row) for row in csv_reader]
        db.add_all(orders_data)
        db.commit()
        return {"message": "CSV data successfully uploaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/order-stats")
def get_order_stats(db: Session = Depends(get_db)):
    orders = db.query(Order).all()
    stats = calculate_order_stats(orders)
    return {"order_stats": stats}
