from sqlalchemy import create_engine , text
engine = create_engine("postgresql://username:password@localhost:5432/postgres")

def number_after_date(mydate):
    with engine.connect() as connection:
        query = text(
            f"SELECT COUNT(*) FROM public.\"dataBors\" WHERE initiationdate > {mydate}"
        )
        result = connection.execute(query)
        for row in result:
            return row[0]

newdate = " '2008-08-11' "
print(number_after_date(newdate))