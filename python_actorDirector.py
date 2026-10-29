import csv 
import sys 

d={} 
#header: tconst,actor,actorName,director,directorName,averageRating,numVotes 
with open('infoActors.csv', newline='', encoding='utf-8') as csvfile: 
    reader = csv.DictReader(csvfile,escapechar='\\') 
    for row in reader: 
        rating=float((row['averageRating'])) 
        t=d.get((row['actor'],row['director']),(row['actorName'], 
        row['directorName'],0,0,rating,rating)) 
        d[(row['actor'],row['director'])] = (row['actorName'], row['directorName'],t[2]+1,t[3]+rating, min(rating,t[4]), max(rating,t[5])) 

d_filtered = [(v[0],v[1],v[2],v[3]/v[2],v[4],v[5]) for k,v in d.items() if v[2]>=2] 
d_ordered=sorted(d_filtered,key=lambda l: -l[2]) 

print(d_ordered[:20]) 
