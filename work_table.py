import psycopg2

from psycopg2 import Error
from settings import *


try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database='shop_db'
    )

    cursor = connection.cursor()

# 1. Show the list of first and last names of the employees from London.
    data = 'select first_name, last_name from employee e where city_id = 1'
    cursor.execute(data)
    connection.commit()
    print('Employees from London: ', cursor.fetchall())
# # 2. Show the list of first and last names of the employees whose first name begins with letter C.

    data = "select first_name, last_name from employee e where first_name like 'C%'"
    cursor.execute(data)
    connection.commit()
    print('Emplyees first names started from "C": ', cursor.fetchall())

# # 3. Show the list of first, last names and ages of the employees whose age is greater than 55. The result should be sorted by last name.

    data = "select first_name, last_name, date_of_birth from employee e where extract(year from now()) - extract(year from date_of_birth) > 55"
    cursor.execute(data)
    connection.commit()
    print('The employees older than 55 are: ', cursor.fetchall())

# 4. Calculate the greatest, the smallest and the average age among the employees from London.

    data = """select min(extract(year from now())- extract(year from date_of_birth)) as "minimum", max(extract(year from now())- extract(year from date_of_birth)) as "maximum", avg(extract(year from now())- extract(year from date_of_birth)) as "average" from employee e where e.city_id in (select id from city c where c.city_name = 'London')"""
    cursor.execute(data)
    connection.commit()
    print("Minimum, maximum, average", cursor.fetchall())


# 5. Show the list of cities in which the average age of employees is greater than 35 (the average age is also to be shown)

    data = """select city_name, round(avg(extract (year from now()) - extract(year from e.date_of_birth))) from city c left join employee e on c.id = e.city_id group by city_name having round(avg(extract (year from now()) - extract(year from e.date_of_birth))) > 35"""
    cursor.execute(data)
    print('List if cities with average age > 35', cursor.fetchall())


# 6. Show first, last names and ages of 3 eldest employees.

    data = """

    """

    print(cursor.fetchall())

    print('Done')
except(Exception, Error) as error:
    print("Error connection: ", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Connection was closed')
