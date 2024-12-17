from sqlalchemy import create_engine , text
engine = create_engine("postgresql://postgres:09101489397@localhost:5432/postgres")

def number_after_date(value):
    with engine.connect() as connection:
        query = text(
            f'SELECT * FROM public."dataBors" WHERE netasset > {value} ORDER BY netasset'
        )
        result = connection.execute(query)
        rows = []
        for row in result:
            rows.append(row)
        return rows

value = 2000000000000
print(number_after_date(value))