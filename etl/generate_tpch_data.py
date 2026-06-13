import random
import csv
import os
from datetime import date, timedelta

random.seed(42)  # so we get the same data every time we run it

OUTPUT_DIR = "./tpch_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 1000
NUM_SUPPLIERS = 100
NUM_PARTS = 500
NUM_ORDERS = 3000
NUM_LINEITEMS_PER_ORDER = 3  # each order gets ~3 line items

MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
SHIP_MODES = ['TRUCK', 'MAIL', 'AIR', 'RAIL', 'SHIP', 'FOB', 'REG AIR']
SHIP_INSTRUCTS = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
PART_TYPES = ['SMALL BRUSHED STEEL', 'LARGE POLISHED BRASS', 'MEDIUM ANODIZED COPPER',
              'STANDARD BURNISHED TIN', 'PROMO PLATED NICKEL']
CONTAINERS = ['SM BOX', 'LG BOX', 'MED BAG', 'JUMBO CAN', 'WRAP CASE']
BRANDS = [f'Brand#{i}{j}' for i in range(1,6) for j in range(1,6)]

def random_date(start_year=1993, end_year=1998):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_phone():
    return f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def random_name(prefix, key):
    return f"{prefix}#{str(key).zfill(9)}"

def random_comment(length):
    words = ['special', 'pending', 'unusual', 'express', 'final', 'regular',
             'silent', 'blithely', 'carefully', 'slyly', 'quickly', 'furiously']
    comment = ' '.join(random.choices(words, k=random.randint(3, 8)))
    return comment[:length]

# --- Generate customers ---
print("Generating customers...")
customers = []
with open(f"{OUTPUT_DIR}/customer.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(['c_custkey','c_name','c_address','c_nationkey','c_phone',
                     'c_acctbal','c_mktsegment','c_comment'])
    for i in range(1, NUM_CUSTOMERS + 1):
        row = [i, random_name("Customer", i), f"{random.randint(1,999)} Main St",
               random.randint(0, 24), random_phone(),
               round(random.uniform(-999.99, 9999.99), 2),
               random.choice(MARKET_SEGMENTS), random_comment(117)]
        writer.writerow(row)
        customers.append(i)

# --- Generate suppliers ---
print("Generating suppliers...")
suppliers = []
with open(f"{OUTPUT_DIR}/supplier.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(['s_suppkey','s_name','s_address','s_nationkey','s_phone',
                     's_acctbal','s_comment'])
    for i in range(1, NUM_SUPPLIERS + 1):
        row = [i, random_name("Supplier", i), f"{random.randint(1,999)} Commerce Ave",
               random.randint(0, 24), random_phone(),
               round(random.uniform(-999.99, 9999.99), 2), random_comment(101)]
        writer.writerow(row)
        suppliers.append(i)

# --- Generate parts ---
print("Generating parts...")
parts = []
with open(f"{OUTPUT_DIR}/part.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(['p_partkey','p_name','p_mfgr','p_brand','p_type',
                     'p_size','p_container','p_retailprice','p_comment'])
    for i in range(1, NUM_PARTS + 1):
        row = [i, random_name("Part", i), f"Manufacturer#{ random.randint(1,5)}",
               random.choice(BRANDS), random.choice(PART_TYPES),
               random.randint(1, 50), random.choice(CONTAINERS),
               round(random.uniform(900, 2099), 2), random_comment(23)]
        writer.writerow(row)
        parts.append(i)

# --- Generate orders + lineitems ---
print("Generating orders and lineitems...")
with open(f"{OUTPUT_DIR}/orders.csv", "w", newline="") as fo, \
     open(f"{OUTPUT_DIR}/lineitem.csv", "w", newline="") as fl:
    
    order_writer = csv.writer(fo)
    order_writer.writerow(['o_orderkey','o_custkey','o_orderstatus','o_totalprice',
                           'o_orderdate','o_orderpriority','o_clerk',
                           'o_shippriority','o_comment'])
    
    lineitem_writer = csv.writer(fl)
    lineitem_writer.writerow(['l_orderkey','l_partkey','l_suppkey','l_linenumber',
                              'l_quantity','l_extendedprice','l_discount','l_tax',
                              'l_returnflag','l_linestatus','l_shipdate',
                              'l_commitdate','l_receiptdate','l_shipinstruct',
                              'l_shipmode','l_comment'])
    
    for i in range(1, NUM_ORDERS + 1):
        order_date = random_date()
        status = random.choice(['O', 'F', 'P'])
        total_price = 0
        
        line_items = []
        for ln in range(1, NUM_LINEITEMS_PER_ORDER + 1):
            part = random.choice(parts)
            supplier = random.choice(suppliers)
            qty = round(random.uniform(1, 50), 2)
            price = round(random.uniform(100, 5000), 2)
            discount = round(random.uniform(0, 0.10), 2)
            tax = round(random.uniform(0, 0.08), 2)
            ship_date = order_date + timedelta(days=random.randint(1, 60))
            commit_date = order_date + timedelta(days=random.randint(30, 90))
            receipt_date = ship_date + timedelta(days=random.randint(1, 30))
            return_flag = random.choice(['R', 'A', 'N'])
            line_status = 'F' if ship_date < date.today() else 'O'
            total_price += price
            line_items.append([i, part, supplier, ln, qty, price, discount, tax,
                               return_flag, line_status, ship_date, commit_date,
                               receipt_date, random.choice(SHIP_INSTRUCTS),
                               random.choice(SHIP_MODES), random_comment(44)])
        
        order_writer.writerow([i, random.choice(customers), status,
                               round(total_price, 2), order_date,
                               random.choice(ORDER_PRIORITIES),
                               f"Clerk#{random.randint(1,1000):09d}",
                               random.randint(0, 1), random_comment(79)])
        
        for line in line_items:
            lineitem_writer.writerow(line)

print(f"Done. Files written to {OUTPUT_DIR}/")
print("Files created:")
for f in os.listdir(OUTPUT_DIR):
    print(f"  {f}")