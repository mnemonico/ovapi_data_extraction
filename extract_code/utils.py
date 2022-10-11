import json
import pandas

with open('/home/osboxes/gitlab/blablacar_usecase1/response.json', 'r') as json_file:
    data = json.load(json_file)

lines = []


#def line_mapper_to_record(data):
for _, key_line in enumerate(data.keys()):
    lines.append({'Line': key_line, **data[key_line]})


#print(lines)

df = pandas.DataFrame(data=lines)
print(df.head(5))
