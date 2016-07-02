r = requests.get('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term=%220000/01/01%22[PDAT]%20:%20%223000/12/31%22[PDAT]&RetMax=3956406&RetStart=0')
soup = BeautifulSoup(r.content, "lxml-xml")
allpmcids = []
for entry in soup.findAll('Id'):
    allpmcids.append(int(entry.contents[0]))

f = open("out.txt","w")
for line in allpmcids:
    f.write('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=')
    f.write(str(line))
    f.write("\n")

#to download all docs in parallel:
#cat out.txt | parallel -j 20 --gnu "wget {}"