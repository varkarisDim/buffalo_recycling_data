import pandas as pd
import matplotlib.pyplot as plt
import csv
import mysql.connector
from getpass import getpass
from operator import itemgetter
from retrieve_data import retrieve_as_df

# this 
# year[1]['total_in_tons'] = pd.to_numeric(year[1]['total_in_tons'], errors='coerce')
# produces a warning flaggign chained assignments
# we overwrite the reference to it and use the converted values to int
# that is why we suppress the warning, it doesn't apply to our situation
pd.options.mode.chained_assignment = None

years = []
recyclable_category = []
# why list and not tuple
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December')

# retrieve the data frame from the given domain and api endpoint
df = retrieve_as_df()

# region data extraction

# filter only the recyclable  materials
recyclable_df = df[(df['type'].str.contains("recycl", case=False, regex=False))]
# remove the incomplete 2022 data
recyclable_df = recyclable_df[recyclable_df['date'] < '2022']

# endregion

# region data processing
# find all the different years that we have data for
# we conver the year to int in order to get it automatically sorted when drawing the plot
for i in recyclable_df.index:
    # c
    if int(recyclable_df.loc[i]['date'][0:4]) not in years:
        years.append(int(recyclable_df.loc[i]['date'][0:4]))
    if (recyclable_df.loc[i]['type'] not in recyclable_category):
        recyclable_category.append(recyclable_df.loc[i]['type'])


# initialize the lists for the data in question, tonnage sums per
# year
yearly_tonnage = [[i, 0] for i in years]
# type
type_tonnage =  [[i, 0] for i in recyclable_category]
# month
monthly_tonnage = [[i, 0] for i in MONTHS]

# populate above lists / create data in question
for i in recyclable_df.index:
    # calculate sum of recyclable per year
    for index, j in enumerate(years):
        if int(recyclable_df.loc[i]['date'][0:4]) == j:
            yearly_tonnage[index][1] += int(recyclable_df.loc[i]['total_in_tons'])
            break
    # calculate sum of recyclable per type
    for index, j in enumerate(recyclable_category):
        if (recyclable_df.loc[i]['type'] == j):
            type_tonnage[index][1] += int(recyclable_df.loc[i]['total_in_tons'])
            break
    # calculate sum of recyclable per month
    for index, j in enumerate(MONTHS):
        if (recyclable_df.loc[i]['month'] == j):
            monthly_tonnage[index][1] += int(recyclable_df.loc[i]['total_in_tons'])
            break
# maybe convert the above list to a dataframe for storage???

# sort descending from most tonnage to least for the monthly sum    
monthly_tonnage.sort(key=itemgetter(1), reverse=True)

#endregion

# region Matplotlib

# seperate data for each axis
# here for data visualization it's ok to use tuples instead of lists
# no more data processing
yearly_plot_x, yearly_plot_y = zip(*yearly_tonnage)
type_plot_x, type_plot_y = zip(*type_tonnage)
# get only the top 5
monthly_plot_x, monthly_plot_y = zip(*monthly_tonnage[:5])

# create a figure 12 by 10 inches
fig = plt.figure(figsize=(12, 9))

# customize the figure window
fig.canvas.manager.window.geometry("+20+20")
fig.canvas.manager.set_window_title('Composite Recycling Figure')
fig.suptitle("Recycling in Buffalo", fontsize=16, fontweight='bold')
fig.tight_layout()

# subplot position and formatting
# subplot for question 1, yearly sum
yearly_plot = plt.subplot2grid((2, 2), (0, 0), colspan=3)
yearly_plot.bar(yearly_plot_x, yearly_plot_y, width=0.6, edgecolor='white', linewidth=0.5)
yearly_plot.set_xticks(yearly_plot_x)
yearly_plot.set_xlabel("Year", bbox={'facecolor': '#93edcb'})
yearly_plot.set_ylabel("Tonnage", bbox={'facecolor': '#93edcb'})
yearly_plot.set_title("Yearly Tonnage", bbox={'facecolor': '#82dce8'})

# subplot for question 2, type sum
type_plot = plt.subplot2grid((2, 2), (1, 0))
type_plot.bar(type_plot_x, type_plot_y, width=0.6, edgecolor='white', linewidth=0.5)
type_plot.set_xlabel("Type", bbox={'facecolor': '#93edcb'})
type_plot.set_ylabel("Tonnage", bbox={'facecolor': '#93edcb'})
type_plot.set_title("Type Tonnage", bbox={'facecolor': '#82dce8'})

# subplot for question 3, monthly sum
monthly_plot = plt.subplot2grid((2, 2), (1, 1))
monthly_plot.bar(monthly_plot_x, monthly_plot_y, width=0.6, edgecolor='white', linewidth=0.5)
monthly_plot.set_xlabel("Month", bbox={'facecolor': '#93edcb'})
monthly_plot.set_ylabel("Tonnage", bbox={'facecolor': '#93edcb'})
monthly_plot.set_title("Monthly Tonnage", bbox={'facecolor': '#82dce8'})

plt.show()

#endregion

# region cvs storage

with open('yearly_tonnage.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(("Year", "Tonnage"))
    writer.writerows(yearly_tonnage)

with open('type_tonnage.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(("Type", "Tonnage"))
    writer.writerows(type_tonnage)

with open('monthly_tonnage.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(("Month", "Tonnage"))
    writer.writerows(monthly_tonnage[:5])

# endregion

# region MySQL storage

# cnx = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="p.jerk.2"
# )

print("Connection will be established with MySQL server at the local host.")

while True:
    db_user = input("Enter MySQL server username: ")
    db_pswd = getpass("Password: ")
    mySQL_port = input("Enter MySQL server localhost port (press RETURN for the default 3306): ") or 3306
    try:
        cnx = mysql.connector.connect(
            host="localhost",
            user=db_user,
            passwd=db_pswd,
            port=mySQL_port
        )
    except mysql.connector.ProgrammingError as err:
        if err.errno == 1045:
            print("Wrong password or username.")

        while True:
            choice = input("Do you wish to try again (y/n): ")
            if choice == "n" or choice == "no":
                exit()
            elif choice == "y" or choice == "yes":
                break
            else: 
                print("Wrong input. Use 'y/yes/n/no'.")
                continue
        continue

    except mysql.connector.DatabaseError as err:
        if err.errno == 2003:
            print(err.msg)
            print("Possibly wrong port was entered. Default localhost port is 3306.")
        
        while True:
            choice = input("Do you wish to try again (y/n): ")
            if choice == "n" or choice == "no":
                exit()
            elif choice == "y" or choice == "yes":
                break
            else: 
                print("Wrong input. Use 'y/yes/n/no'.")
                continue
        continue

    print(f"Successful connection to MySQL server at local host:{mySQL_port}")
    break


cnx_cursor = cnx.cursor()

cnx_cursor.execute("CREATE DATABASE IF NOT EXISTS RecyclingDB")

db_cnx = mysql.connector.connect(
    host="localhost",
    user=db_user,
    passwd=db_pswd,
    database="recyclingdb"
)

print("Successful connection to database recyclingdb")

db_cursor = db_cnx.cursor(buffered=True)
db_cursor.execute("CREATE TABLE IF NOT EXISTS yearly_tonnage (Year INT PRIMARY KEY, Tonnage INT)")
db_cursor.execute("CREATE TABLE IF NOT EXISTS type_tonnage (Type VARCHAR(255) PRIMARY KEY, Tonnage INT)")
db_cursor.execute("CREATE TABLE IF NOT EXISTS monthly_tonnage (Month VARCHAR(255) PRIMARY KEY, Tonnage INT)")

insert_query = """INSERT INTO yearly_tonnage (Year, Tonnage) VALUES(%s, %s) ON DUPLICATE KEY UPDATE Tonnage=%s"""
for i in yearly_tonnage:
    record = (i[0], i[1], i[1])
    db_cursor.execute(insert_query, record)

insert_query = """INSERT INTO type_tonnage (Type, Tonnage) VALUES(%s, %s) ON DUPLICATE KEY UPDATE Tonnage=%s"""
for i in type_tonnage:
    record = (i[0], i[1], i[1])
    db_cursor.execute(insert_query, record)

insert_query = """INSERT INTO monthly_tonnage (Month, Tonnage) VALUES(%s, %s) ON DUPLICATE KEY UPDATE Tonnage=%s"""
for i in monthly_tonnage[:5]:
    record = (i[0], i[1], i[1])
    db_cursor.execute(insert_query, record)

db_cnx.commit()

cnx_cursor.close()
db_cursor.close()
cnx.close()
db_cnx.close()

# endregion