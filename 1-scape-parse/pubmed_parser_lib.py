import datetime

class EFetchExtractor(object):
    def __init__(self, soup):
        self.soup = soup
    
    def pmid(self):
        articleids = self.soup.find('article-meta').find_all('article-id')
        for id in articleids:
            if id.attrs['pub-id-type'] == 'pmid': return int(id.contents[0])
            
    def pmcid(self):
        articleids = self.soup.find('article-meta').find_all('article-id')
        for id in articleids:
            if id.attrs['pub-id-type'] == 'pmc': return int(id.contents[0])
    
    def journal_id(self):
        return unicode(self.soup.find('journal-meta').find('journal-id').contents[0])
    
    def journal_id_hash(self):
        return hash(self.soup.find('journal-meta').find('journal-id').contents[0])
    
    def subject(self):
        return unicode(self.soup.find('article-meta').find('article-categories').find('subj-group').find('subject').contents[0])
    
    def title(self):
        return unicode(self.soup.find('article-meta').find('title-group').find('article-title').contents[0]).replace("\n"," ")
        
    def authors(self):
        #Returns nested list containing first names and last names of authors
        lastname = self.soup.find('contrib-group').find_all('surname')
        firstname = self.soup.find('contrib-group').find_all('given-names')
        ln = []
        fn = []
        for entry in lastname:
            ln.append(entry.contents)
        for entry in firstname:
            fn.append(entry.contents)
        return fn, ln
    
    def authorsv2(self):
        lastname = self.soup.find('contrib-group').find_all('surname')
        firstname = self.soup.find('contrib-group').find_all('given-names')
        if len(firstname) != len(lastname): return [] #if names missing, skip authors for this entry
        ln = []
        fn = []
        for entry in lastname:
            ln.append(entry.contents)
        for entry in firstname:
            fn.append(entry.contents)
        output = []
        for i in range(len(ln)):
            if fn[i] == []:
                output.append(['', ln[i][0]])
            else:
                output.append([fn[i][0], ln[i][0]])
        return output
    
    def authorsAfil(self):
        lastname = self.soup.find('contrib-group').find_all('surname')
        firstname = self.soup.find('contrib-group').find_all('given-names')
        afilkey = self.soup.find('contrib-group').findAll('xref', attrs={'ref-type': 'aff'})
        if len(firstname) != len(lastname): return [] #if names missing, skip authors for this entry
        ln = []
        fn = []
        #ak = []
        for entry in lastname:
            ln.append(entry.contents)
        for entry in firstname:
            fn.append(entry.contents)
        #for entry in afilkey:
        #    ak.append(entry.contents)
        afil = []
        if afilkey != []:
            for key in afilkey:
                try:
                    afil.append(self.soup.find('aff', attrs={'id': key['rid']}).getText())
                except:
                    afil = []
                    for entry in lastname:
                        afil.append('')
                    break
        else:
            for entry in lastname:
                afil.append('')
        output = []
        for i in range(len(ln)):
            if len(fn) != len(afil):
                if fn[i] == []: output.append(['', ln[i][0], ''])
                else: output.append([fn[i][0], ln[i][0], ''])
            elif fn[i] == []:
                output.append(['', ln[i][0], afil[i]])
            else:
                output.append([fn[i][0], ln[i][0], afil[i]])
        return output
    
#    def authorsfn(self):
#        #Returns list containing first names
#        firstname = self.soup.find('contrib-group').find_all('given-names')
#        fn = []
#        for entry in firstname:
#            fn.append(entry.contents)
#        return fn
#    
#    def authorsln(self):
#        #Returns list containing last names of authors
#        lastname = self.soup.find('contrib-group').find_all('surname')
#        ln = []
#        for entry in lastname:
#            ln.append(entry.contents)
#        return ln
        
    def pubdate(self):
        day = self.soup.find('pub-date').find('day').getText()
        month = self.soup.find('pub-date').find('month').getText()
        year = self.soup.find('pub-date').find('year').getText()
        return datetime.date(int(year), int(month), int(day))
        
    def acceptdate(self):
        raise NotImplementedError
        
    def abstract(self):
        if self.soup.find('abstract') != None:
            return self.soup.find('abstract').getText()
        
    def references(self):
        raise NotImplementedError
        
    def referencespmidsonly(self):
        refs = []
        for entry in self.soup.find_all('citation'):
            pmid = entry.find('pub-id', {"pub-id-type" : "pmid"})
            if pmid != None:
                if len(str(int(pmid.contents[0]))) <= 8: 
                    #PMIDs between 1 and 8 nums, some bad PMIDs are super long, e.g.
                    #http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=3153861
                    refs.append(int(pmid.contents[0]))
        for entry in self.soup.find_all('mixed-citation'):
            pmid = entry.find('pub-id', {"pub-id-type" : "pmid"})
            if pmid != None:
                if len(str(int(pmid.contents[0]))) <= 8: 
                    refs.append(int(pmid.contents[0]))
        for entry in self.soup.find_all('element-citation'):
            pmid = entry.find('pub-id', {"pub-id-type" : "pmid"})
            if pmid != None:
                if len(str(int(pmid.contents[0]))) <= 8: 
                    refs.append(int(pmid.contents[0]))
        return refs
    
    def keywords(self):
        keywords = []
        if self.soup.find('kwd-group')==None:
            return keywords
        else:
            for entry in self.soup.find('kwd-group').find_all('kwd'):
                keywords.append(entry.getText()) #getText removes formatting tags like <italic>
            return keywords