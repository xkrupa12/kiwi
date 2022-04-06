import csv


def read_csv(filename):
    with open(filename, 'r', newline='') as csvfile:
        datareader = csv.reader(csvfile, delimiter=';')
        for row in datareader:
            yield row

    return

file = 'bids-sample.csv'
for row in read_csv(file):
    print(row)
