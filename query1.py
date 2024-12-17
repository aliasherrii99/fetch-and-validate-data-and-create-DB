from sqlalchemy import create_engine , text
engine = create_engine("postgresql://username:password@localhost:5432/postgres")

# choose row with regno
def show_with_regno(regno):
    with engine.connect() as connection:
        query = text(
            f'SELECT * FROM public."dataBors" WHERE regno = {regno}'
                     )
        result = connection.execute(query)

        for row in result:
            return row

print(show_with_regno(10615))