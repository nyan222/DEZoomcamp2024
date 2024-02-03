##1
```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

sum_gen = 0
n = 0
for sqrt_value in generator:
    n = n + 1
    print(str(n) + '. '+ str(sqrt_value))
    sum_gen = sum_gen + sqrt_value
    if n == 5:
        print('limit 5 sum_gen = ' + str(sum_gen))
```

##2
```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

#sum_gen = 0
n = 0
for sqrt_value in generator:
    n = n + 1
    print(str(n) + '. '+ str(sqrt_value))
    #sum_gen = sum_gen + sqrt_value
    #if n == 5:
        #print('limit 5 sum_gen = ' + str(sum_gen))
```

##Prereq
```python
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)
```


##3
```python
import dlt

p_pipeline = dlt.pipeline(destination='duckdb', dataset_name='all_people')

info = p_pipeline.run(people_1(),
                             table_name="all_people",
                             write_disposition="replace")

print(info)

info = p_pipeline.run(people_2(),
                             table_name="all_people",
                             write_disposition="append")

print(info)
```

```python
import duckdb

conn = duckdb.connect(f"{p_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{p_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data

print("\n\n\n all_people table below:")

people = conn.sql("SELECT * FROM all_people").df()
display(people)

print("\n\n\n stream_download table below:")

sum_age = conn.execute("SELECT SUM(age) as s FROM all_people").df()
print(sum_age)
print(f"\n\nSum of ages from people_1 and people_2 generators is : {sum_age['s'].iloc[0].astype(int)}")
```

##4
```python
pm_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_merged')

info = pm_pipeline.run(people_1(),
                             table_name="people_merge",
                             write_disposition="replace",
                             primary_key="ID")

print(info)

info = pm_pipeline.run(people_2(),
                             table_name="people_merge",
                             write_disposition="merge",
                             primary_key="ID")

print(info)
```

```python
conn = duckdb.connect(f"{pm_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{pm_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data

print("\n\n\n people_merge table below:")

people = conn.sql("SELECT * FROM people_merge order by id").df()
display(people)

print("\n\n\n stream_download table below:")

sum_age = conn.execute("SELECT SUM(age) as s FROM people_merge").df()##.fetchone()[0]
print(sum_age)
print(f"\n\nSum of ages from people_1 and people_2 generators is : {sum_age['s'].iloc[0].astype(int)}")
```