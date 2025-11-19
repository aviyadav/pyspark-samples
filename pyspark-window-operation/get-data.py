import requests

path = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
req = requests.get(path)
url_content = req.content.decode('utf-8')

csv_file_name = "dataset/owid-covid-data.csv"
csv_file = open(csv_file_name, "wb")

csv_file.write(url_content.encode('utf-8'))
csv_file.close()

