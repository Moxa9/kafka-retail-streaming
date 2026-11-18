from src.db import connect


def main():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT window_start, category, event_count, units_sold, revenue
                           FROM sales_minute_aggregate
                           ORDER BY window_start DESC, revenue DESC
                           LIMIT 30""")
            rows = cur.fetchall()
        print("window_start | category | events | units | revenue")
        for row in rows:
            print(" | ".join(str(value) for value in row))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
