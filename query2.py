from sqlalchemy import create_engine , text
engine = create_engine("postgresql://postgres:09101489397@localhost:5432/postgres")

# choose row with regno
list_of_fundstype = list()
with engine.connect() as connection:
    query = text(
        'SELECT DISTINCT fundtype FROM public."dataBors" ORDER BY fundtype'
                 )
    result = connection.execute(query)
    for row in result:
        list_of_fundstype.append(row[0])


def show_AUM(fund_type):
    with engine.connect() as connection:
        query = text(
            f'SELECT SUM(netasset) FROM public."dataBors" WHERE fundtype = {fund_type}'
        )
        result = connection.execute(query)
        for row in result:
            return row[0]

for i in list_of_fundstype:
    print('type is : ' ,i ,'  sum of asset  :' , show_AUM(i))
