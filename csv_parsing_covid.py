import csv
from datetime import datetime


def read_csv(filename):
    with open(filename, 'r', newline='') as csvfile:
        datareader = csv.reader(csvfile, delimiter=';')
        for row in datareader:
            yield row

    return


file = 'covid-sample.csv'

f = open('result_set.csv', 'w')
writer = csv.writer(f)

headers: list = []
filtered_rows: list = []
desired_keys = ['Date', 'CountryName', 'Jurisdiction']

for row in read_csv(file):
    # 1. riadok je hlavicka
    if not headers:
        headers = row
        writer.writerow(desired_keys)
        continue

    # transform array to dict for easier handling
    dictionary = dict(zip(headers, row))

    # chcem vyselektovat z csv len riadky NAT_TOTAL v stpci Jurisdiction (tykaju sa krajin a nie statov - napr. USA je NAT ale napr. Washington ci Texas je tam ako STATE a to ma nezaujima a potrebujem to odfiltrovat prec)
    if dictionary['Jurisdiction'] != 'NAT_TOTAL':
        continue

    # vybrat si len niektore stlpce z toho csv, lebo mnohe su pre mna zbytocne (toto som dokazala spravit a nakopirovat to do noveho csv, posielam kod)
    subset = {k: dictionary[k] for k in desired_keys}

    # a potom stlpce "Date", ktory je v string formate 'YYYYMMDD' zmenit na datumovy format MM/DD/YY
    subset['Date'] = datetime.strptime(subset['Date'], '%Y%m%d').strftime('%m/%d/%y')

    writer.writerow(subset.values())

f.close()
exit()