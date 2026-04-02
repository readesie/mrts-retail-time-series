#Module 8 Video 8.2 example
import csv

#open file
with open('users.csv') as csv_file:
    #read csv file
    csv_reader = csv.reader(csv_file, delimiter=',')

    #loop through data
    for row in csv_reader:
        print(row)
