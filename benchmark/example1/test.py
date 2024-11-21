import pandas as pd

data = {
    "city": ["New York", "Los Angeles", "New York", "Los Angeles"],
    "year": [2010, 2010, 2020, 2020],
    "population": [8.1, 3.8, 8.3, 4.0]
}
df = pd.DataFrame(data)
df.set_index(["city"], inplace=True)

print(df)

indices = ['New York', 'Los Angeles']
df = df.loc[indices]
print(df)
