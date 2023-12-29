import csv
from typing import List

def read_csv_file(file_path: str) -> List[dict]:
    orders = []
    with open(file_path, newline="") as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            amount = float(row["price"])
            order = {"amount": amount}
            orders.append(order)
    return orders

def calculate_order_stats(orders: List[dict]) -> dict:
    total_orders = len(orders)
    total_amount = sum(order["amount"] for order in orders)
    return {"total_orders": total_orders, "total_amount": total_amount}

if __name__ == "__main__":
    file_path = "orders.csv"
    orders_data = read_csv_file(file_path)
    stats = calculate_order_stats(orders_data)
    print(stats)
