# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 17:04:18 2017

@author: Mike
"""

import requests, os, zipfile, csv, json, sys
from bs4 import BeautifulSoup


def get_urls(code):
    """ Take a dataset ID and returns the urls for that dataset,
    can inlcude multiple returns, de-referencable by geography and time"""
    
    # request list of all datasets on the API
    url = 'http://data.ons.gov.uk/ons/api/data/datasets.xml?apikey=Y6Xs59zXU0&'
    r  = requests.get(url)
    
    # Parse it, get rid of any not detail .json links
    soup = BeautifulSoup(r.content, 'lxml')
    all_details = soup.find_all('url', {'representation':'xml'})
    
    # Get our boy
    found = [x.text for x in all_details if '/' + code + '.xml' in x.text]
    
    return found


def get_csv_url(url, english=True):
    """ Returns the pre-canned version of a dataset as csv from WDA,
    by default its english though calling with english=False will change that"""
    
    url = 'http://data.ons.gov.uk/ons/api/data/' + url
    r  = requests.get(url)
    
    # Parse it, get rid of any not detail .json links
    soup = BeautifulSoup(r.content, 'lxml')
    all_details = soup.find_all('document', {'type':'CSV'})
    
    # English or Welsh
    if english:
        csv_url = [x.text for x in all_details if 'EN.zip' in x.text]
    if not english:
        csv_url = [x.text for x in all_details if 'CY.zip' in x.text]
    
    # get rid of any crap after .zip thatll interfere with future request
    csv_url = csv_url[0].split('.zip')[0] + '.zip'
    
    return csv_url
    

def unpackAndGetName(zip_file_url):

    # modified from: http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    def download_file(url):
        local_filename = 'tempfile.zip'
        # NOTE the stream=True parameter
        r = requests.get(url, stream=True)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return local_filename

    download_file(zip_file_url)
    
    # Extract it and get names of all files
    dlzip = zipfile.ZipFile("tempfile.zip", 'r')
    dlzip.extractall(os.getcwd())
    zipnames = dlzip.namelist()
    dlzip.close()
    
    csvName = [x for x in zipnames if '.csv' in x]
    assert len(csvName) == 1, "More than 1 csv in pre-canned zip"
    
    return csvName[0]


""" Grabs everything we'll need from dataset details """
def buildStructureDict(url):
    url = 'http://data.ons.gov.uk/ons/api/data/' + url.replace('.xml', '.json')
    r  = requests.get(url)
    
    # Parse it, yank the dimensions code
    jsonAsDict = json.loads(r.text)
    all_dims = [x for x in jsonAsDict['ons']['datasetDetail']['dimensions']['dimension']]
    
    dimList = []
    name = ''
    for dim in all_dims:
        code = dim['dimensionId']
        if type(dim['dimensionTitles']['dimensionTitle']) == list:
            name = dim['dimensionTitles']['dimensionTitle'][0]['$']
        else:
            name = dim['dimensionTitle']['$']
        assert name != '', "Cannot identify CL_ name from dataset details"        
        dimList.append((code, name))

    # grab the obs count for validation
    obsCount = jsonAsDict['ons']['datasetDetail']['obsCount']  
    
    """
    -------------------------
    This is what we're making
    -------------------------
    StructureDict = {    
                    'Animals' = { 
                             'code': 'CL_1891828',
                             'geography': '2014 Administrative Geography',
                             'codeList : {
                                         'Cats':'CI_121312',
                                         'Dogs':'CI_912101'
                                         }
                             },
                    etc.....
                    }
    """
    
    knownHierarchies = ['2011WKWZH','2011PARISH','2015EUROSTATH','2014WARDH','2011STATH',
                        '2011WARDH','2011PCONH','2013WARDH','2011NAWH','2011CMLADH',
                        '2013HEALTHH','2011HTWARDH','2012WARDH','2011NAPH','UKWARD','2011BUAH']
    
    hierarchy = ''
    structureDict = {}
    for dim in dimList:
        
        code = dim[0]
        name = dim[1]

        if code in knownHierarchies:
            hierarchy = code

        # Build the generic url (cant use specific as some are broken/duplicated)
        if "context=Social" in url:
            context = "Social"
        if "context=Economic" in url:
            context = "Economic"
        if "context=Census" in url:
            context = "Census"
        newurl = "http://web.ons.gov.uk/ons/api/data/classification/{cl}.json?apikey=Y6Xs59zXU0&context={cont}".format(cont=context,cl=code) 
                
        codeData = requests.get(newurl)

        itemCodeList = {}
        try:
            codeListAsDICT = json.loads(codeData.text)
        except:
            pass # its geography, its blocked
        
        try:
            if True:  
                
                # Codelists are sometime a Dict, sometimes a list of dicts...because.
                if type(codeListAsDICT['Structure']['CodeLists']['CodeList']) == list:
                    tryAll = codeListAsDICT['Structure']['CodeLists']['CodeList']
                elif type(codeListAsDICT['Structure']['CodeLists']['CodeList']) == dict:
                    tryAll = []
                    tryAll.append(codeListAsDICT['Structure']['CodeLists']['CodeList'])
                else:
                    # TODO - throw an error!
                    print('CodeList must be List or Dict')
                
                itemCodeList = {}
                assert type(tryAll) == list

                foundLists = []
                for listItem in tryAll:
                    
                    # try every possible codelist for a match as theyre not differentiated in any way on the API. ffs!
                    try:
                        
                        # catch for 'lists' of 1 item.....sigh
                        if type(listItem['Code']) != list:
                            listItem['Code'] = [listItem['Code']]
                        
                        for item in listItem['Code']:
                            # try catch as not everything has a welsh name
                            try:
                                n = item['Description'][0]['$']
                            except:
                                n = item['Description']['$']

                            c = item['@value']

                            itemCodeList.update({n:c})
                            
                        # Need to adress the spectre of codelists with identical keys but different values
                        if len(foundLists) == 0:
                            foundLists.append(itemCodeList)
                        else:
                            # always compare to 1st. if any 2 match we're knackered anyway
                            for key in foundLists[0].keys():
                                if foundLists[0][key] != itemCodeList[key]:
                                    raise ValueError('Multiple Codelists match the classification', code)
                            if len(foundLists) != len(itemCodeList):
                                raise ValueError('Returning multiple valid codelists for ' + code + ', operation aborted')
                                  
                    except:
                        pass #its in here somewhere
                        
        except:
            assert code in knownHierarchies, 'Failed to build codelist for ' + str(code) + ' ' + newurl
            
        structureDict.update({name:{'code':code, 'codeList':itemCodeList}})
    
    return hierarchy, structureDict, obsCount
    
     
def bodytransform(acsv, structureDICT, hierarchy, obsCount, censusOverride, rerun=False):

    # if we're adding a row and it seems the wront length.... feedback
    def throwRowLengthError(dimDict, row, i, justDims, justItems, timeDim, timeItem, oldrow, newrow, structureDICT):

        # dump the lookups so we can find out what went wrong
        with open('debug-Dict.json', 'w') as debugDICT:
            json.dump(structureDICT, debugDICT)
            
        print ('\nError: Generating row of unexpected length')
        print ('--------------------------------------------')
        print ('The structureDICT (lookups) have been output as debug-Dict.json:', '\n')
        print ('Row length expected/got: ', oldrow, newrow)
        print ('Dimensions Cell: ', str(dimDict['dimensions'][i]))
        print ('Dimensions Found:' + str(justDims) + '\n')
        print ('Items Cell: ', str(dimDict['items'][i]))
        print ('Items Found:' + str(justItems) + '\n')
        print ('Chosen item Dim/Item: ', timeDim, timeItem)
        print ('Target Cell: ', row[i])
        print ('--------------------------------------------')
        print('Killing process')
        
        # kill the script, we dont want to output this mess
        sys.exit()
        
        
    # Keeps track of previous 4 rows read
    dimDict = {1:'', 2:'', 3:'', 4:''}
    def shuffle(row, dimDict):
        dimDict[4] = dimDict[3]
        dimDict[3] = dimDict[2]
        dimDict[2] = dimDict[1]
        dimDict[1] = tuple(row)   # tuple so it cant mutate

    # Returns a count of columns to skip before hitting observations
    def findBlanks(row, index=0):
        if row[index] == '':
            index += 1
            return findBlanks(row, index)
        else:
            return index
                
    # Splits out the time dimension from the rest
    def splitOutTime(dims, items):
        
        dims = tuple(dims.split('~'))  # eg:  Cats~Dogs becomes ['Cats', 'Dogs'] 
        items = tuple(items.split('~'))
        
        doTime = False
        timeNames = ['year', 'Year', 'Month', 'month', 'Quarter', 'quarter', 'Time', 'time']
        for i in range(0, len(dims)):
            if dims[i] in timeNames:
                timeNum = i
                doTime = True
                
        if doTime:
            justDims = ()
            justItems = ()
            for i in range(0, len(items)):
                if i != timeNum:

                    justDims = justDims + (dims[i],)
                    justItems = justItems + (items[i],)
                else:
                    timeDim = dims[i]
                    timeItem = items[i]

                    # 'time' is too generic for time dimensoins, 
                    # derrive it from timeItem
                    if timeDim == 'Time' or timeDim =='time':
                        timeDim, timeItem = cleanTimeString(timeItem)
                        timeDim = timeDim.capitalize()

            return timeDim, timeItem, justDims, justItems
        
        return '', '', dims, items
        
    # Splits out the dim1 dim2 nonsence and gets rid of Total: Sub-total etc       
    def contentSplit(dims, items, structureDICT):
        
        assert len(dims) == len(items), "Uneven split in Items and Dimensions"

        cols = () # tuple as order is critical
        for i in range(0, len(dims)):
            
            """
            WDA is a quagmire of text strings that dont -quite- match up.
            The following is a list of variations to try when trying to match the in-csv text to the text in classifications API
            """
            # TODO - there must be a better way
            
            # A list with one variation, we'll add more as we go then try the lot
            attempts = [items[i]]

            # if 'Total:  ', clean up and try variations
            if items[i][:7] == 'Total: ':
                noTotal = items[i].replace('Total: ', '').strip()
                
                # 'Total ' prefix removed then' + dimension name pluralised
                pluralCombineTotal = str(noTotal + ' ' + dims[i] + 's')  # add Pluralise
                attempts.append(pluralCombineTotal)
                
                # 'Total ' prefix removed then' + dimension name                      
                singleCombineTotal = str(noTotal + ' ' + dims[i])  # add singular
                attempts.append(singleCombineTotal)
                
                # 'Total ' prefix removed
                attempts.append(noTotal)

            # UKBAF01 Handling
            noMeasure = items[i].replace('Â£', '')
            noMeasure = noMeasure.replace('£', '')
            noMeasure = noMeasure.replace('(Thousands)', '').strip()
            attempts.append(noMeasure)
            
            """
            catches for bad encoding, welsh names etc
            for now we'll slip them in as a new variation/attempt and refactor wth the rest later
            """
            # TODO - waaaaay too hacky
            if str(items[i]) == '2223 : Manufacture of buildersÃ\u2020 ware of plastic': attempts.append("2223 : Manufacture of buildersÆ ware of plastic")
            if str(items[i]) == '2223 : Manufacture of buildersï¿½ ware of plastic': attempts.append("2223 : Manufacture of buildersÆ ware of plastic")
            
            
            # make the triple
            Seeking = True
            for attempt in attempts:
                if Seeking:
                    try: # keep trying to pattern match                        
                        cols = cols + (structureDICT[dims[i]]['code'], dims[i].capitalize(), structureDICT[dims[i]]['codeList'][attempt])  # the '' is a space for for CL_
                        Seeking = False    # we've found what we're looking for
                    except:
                        pass   # try the next one

            # if we still havnt found it - bomb out
            if Seeking:
                raise ValueError ('Cant find a match for <' + items[i] + '> in StructureDICT\n\n', structureDICT)
                
        return cols
        
    
    # Finds the horozonal index for 'Geographic ID. Uses slipcols to narrow down search
    def findGeoIndex(itemrow, skipcols):
        
        foundG = False
        for i in range(0, skipcols):
            if itemrow[i] == 'Geographic ID':
                gindex = i
                foundG = True
        
        assert foundG, "'Header 'Geographic ID' not found"
        return gindex
        

    """ Creates the first triple out of the observation 'cell' """
    def splitObs(cell):
        
        # settle on cv or no
        if '[CV' in cell:
            cellbits = cell.split('[CV')
            cell = cellbits[0].strip()
            cv = cellbits[1].replace(']', '').strip()
        else:
            cv = ''
        
        try:
            cell = float(cell)
            ob = cell
            dm = ''
        except:
            dm = cell
            ob = ''
  
        return ob, dm, cv


    def cleanTimeString(cell):
        
        try:
            # is it just a year?
            cell = int(cell)
            return 'Year', cell
        except:
            pass # its not that simple
            
        # quarters
        quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        for q in quarters:
            if q in cell:
                return 'Quarter', cell
        
        # months
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
                  'Aug', 'Sep' 'Oct', 'Nov', 'Dec']
        for m in months:
            if m in cell or m.upper()  in cell or m.lower() in cell:
                return 'Months', cell
    
        # if we cant identify return blanks
        return '', ''

        
    # WDA only has a finite number of differentiators in use, and thet somtimes define the
    # time metric for the whole dataset, so get them here
    def processDifferentiatorForTime(cell):

        # A list of non standard non time specific differentiators from WDA
        # We'll sort this out later, possibly case by case /sigh        
        nonSpecificDiff = ['Population','D16', 'Health', 'Service', 'Migration',
                           'Months', 'Agriculture','Quarters','Construction','NonFinancialServices',
                           'Deaths','Years','Births', 'Production','Distribution'] 
        
        assert cell not in nonSpecificDiff, "Non Specific Time Differentiator or not in 'cell' A3. Operation Aborted"
        
        SpecificDiffs = {
                        '2014':['2014', 'Year'],
                        '2004':['2003', 'Year'],
                        '2012':['2012', 'Year'],
                        '2015':['2015', 'Year'],
                        '2016Q3':['Q3 2016','Quarter'],
                        '2002':['2002', 'Year'],
                        'Dec2016':['Dec 2016', 'Month'],
                        '2003':['2003', 'Year'],
                        '2009':['2009', 'Year'],
                        '2007':['2007', 'Year'],
                        '2006':['2006', 'Year'],
                        'Q32016':['Q3 2016', 'Quarter'],
                        '2013':['2013', 'Year'],
                        '2016':['2016', 'Year'],
                        '2005':['2005', 'Year'],
                        '2008':['2008', 'Year'],
                        '2011':['2011', 'Year'],
                        '2010':['2010', 'Year'], 
                         }
        return SpecificDiffs[cell][1], SpecificDiffs[cell][0]


    """
    CSV Build
    """       
    with open(acsv, 'r') as source:
        myreader = csv.reader(source, delimiter=',', quotechar='"')
        
        found = False
        count = 0
        obOutCount = 0
        timeIndex = False     # column number for the date
        
        # get rid of any old verisons so windows doesnt moan
        try:
            os.remove(filename[11:]) # del previous version if applicable
        except:
            pass
        
        # Write the new CSV
        with open('Incomplete-V3_' + acsv, 'w', newline='') as targetfile:
            mywriter = csv.writer(targetfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            for row in myreader:
                count += 1
                
                try:
                    if count == 3:
                        differentiator = row[0] 
                except:
                    differentiator = False   # it fine, not all datasets have one
                
                # find geographic id somwhere near the beginning of the row
                for i in range(0, 5):
                    try:
                        if 'Geographic ID' in row[i]:
                    
                            found = True
                            shuffle(row, dimDict)
                            
                            # Now we've identified the lines; switch to somthing semantically meaningful
                            dimDict['items'] = dimDict.pop(1)
                            dimDict['dimensions'] = dimDict.pop(2)
                            dimDict['unitOfMeasure'] = dimDict.pop(3)
                            dimDict['measureType'] = dimDict.pop(4)
        
                            # hacky 1 line skip, we dont want to process this heder row as obs
                            doHeader = True
                            
                            """ Since this effectivly marks the point we're ready, we'll
                            get some other structural information as well"""
        
                            # How many left had columns are there before the obserbvations start
                            skipcols = findBlanks(dimDict['dimensions'])
                            
                            geoIndex = findGeoIndex(dimDict['items'], skipcols)
                            assert geoIndex != i, "GeoIndex Function pointless, use i"
                            
                    except:
                        pass  # to stopit falling over if we an index a csv 'cell' that doesnt exist
                        
                    
                    # Time will always be to left of geography so no need to worry about anything else
                    # i,e this is optional, Geographic Id isnt, so the above code will proc in a cell or two anyway
                    try:
                        if 'Time' in row[i]:
                            timeIndex = i
                    except:
                        pass  # to stop it falling over if we an index a csv 'cell' that doesnt exist
                        

                # keep track of last 4 rows if still looking for headers
                if not found:
                    shuffle(row, dimDict)
                
                if found and doHeader:
                    
                    # We're about to start make a headerrow
                    headerRow = ['Observation','Data_Marking','Observation_Type_Value']
                    dimSample = row[skipcols+1].split('~')
                    numberOfDims = len(dimSample)
                    
                    # TODO - too hacky. not DRY!
                    timeWords = ['year', 'Year', 'Month', 'month', 'Quarter', 'quarter', 'Time', 'time']
                    if len([x for x in dimSample if x in timeWords]) == 0 and timeIndex == False:
                        numberOfDims += 1 # account for missing time dim
                    # elif timeIndex != False:   # use elif so we dont account for this twice
                    #    numberOfDims += 1
                    
                    for i in range(1, numberOfDims+2):
                        headerRow.append('Dimension_Hierarchy_' + str(i))
                        headerRow.append('Dimension_Name_' + str(i))
                        headerRow.append('Dimension_Value_' + str(i))
                        doHeader=False
                    
                elif found and len(row) < skipcols:
                    pass # its a bloody footer, do nothing
                    
                elif found:

                    # for all the columns with observations.....
                    for i in range(skipcols, len(row)):

                        # no point in a blank obs, unless its the 'include blanks' rerun
                        # have to filter out any '(C) Crown Copyright' footers as it breaks the observation count validation
                        if row[i] != '' or rerun == True:     
                            
                            obOutCount += 1
                            newrow = []
    
                            # Obs triple
                            ob, dm, cv = splitObs(row[i])
                            newrow.append(ob)      
                            newrow.append(dm)        
                            newrow.append(cv) 
                            
                            # X ----------------------------------------
                            # TIME
                            # TODO - clearly should be ina  funcgtion
                            
                            # split out any time dimension
                            timeDim, timeItem, justDims, justItems = splitOutTime(dimDict['dimensions'][i], dimDict['items'][i])
                            
                            # if we're still looking - do we have a time string in initial CSV columns?
                            if timeDim == '' and timeIndex is not False:
                                timeDim, timeItem = cleanTimeString(row[timeIndex])
                            
                            # if we still dont have time, try and get it from the differentiator
                            if timeDim == '':
                                if differentiator != False:
                                    timeDim, timeItem = processDifferentiatorForTime(differentiator)
                            
                            # Failing that override with census. throw an error if we've already got time (should be impossible)
                            if censusOverride:
                                assert timeDim == '', "Showing as Census but has explicit time"
                                timeDim = 'Decennial'
                                timeItem = '2011'
                                
                            assert timeDim != '', 'Unable to identify time triple' + dimDict['dimensions'][i] + ' ' + dimDict['items'][i]
                            
                            # X ------------------------------------------
                            
                            newrow.append('time')
                            newrow.append(timeDim)
                            newrow.append(timeItem)
        
                            # Geo triple
                            newrow.append(hierarchy)
                            newrow.append('Geography')
                            newrow.append(row[geoIndex])
                            
                            # add dimension triples             
                            for cell in contentSplit(justDims, justItems, structureDICT):
                                newrow.append(cell)
                            
                            # output head if needed
                            if headerRow != []:
                                mywriter.writerow(headerRow)
                                headerRow = []
                                expectedRowLength = len(newrow)
                                
                            # output processed obs
                            if len(newrow) != expectedRowLength: throwRowLengthError(dimDict, row, i, justDims, justItems, timeDim, timeItem, str(len(newrow)), expectedRowLength, structureDICT)
                            mywriter.writerow(newrow)
                        
    targetfile.close()
    source.close()         
    # If the obs dont match the count on WDA, rerun assuming we actually want those blank 'cells'
    if int(obOutCount) != int(obsCount) and rerun == False:
        print('ObCountError: Assuming no blank observation cells returned an unexpected observations count', obOutCount, ' instead of ', obsCount)
        print('Attempting V3 Transformation with blanks assumed. Assertion error if this fails.') 
        bodytransform(acsv, structureDICT, hierarchy, obsCount, censusOverride, rerun=True)
        
    else:    
        assert int(obOutCount) == int(obsCount), str(obOutCount) + '!=' + str(obsCount) + "Output rows to not match number of observations in dataset"

    if int(obOutCount) == int(obsCount):
        # it does otherwise we wouldnt get here, but dont want to print this twice
        print ('Sucess: transformed ' + str(obOutCount) + ' of ' + str(obsCount) + ' observations.')    

"""
Main
"""
# TODO - all a bit crap really
identifier = sys.argv[1]

urls = get_urls(identifier)

print('')
print ('--------------')
print(identifier)
print ('--------------')
print('Found the following files for this Identifier:')
for f in urls:
    print(f)
print('')

for ddurl in urls:
        
        url = get_csv_url(ddurl)

        print ('Creating Structural Dictionary')
        # create a dictioary of structural info
        hierarchy, structureDICT, obsCount = buildStructureDict(ddurl)
        
        print('Beginning Processing of:')
        print(url)
        
        print ('Fetching and unpacking pre-canned CSV')
        # Unpack the chosen zip and retutn its name
        targetCSV = unpackAndGetName(url)
        
        # Do we need to override as census?
        if 'context=Census' in ddurl:
            censusOverride = True
        else:
            censusOverride = False
        print('Census override:', censusOverride)
        
        
        print ('Tranforming CSV to V3')
        # transform the chosen csv
        bodytransform(targetCSV, structureDICT, hierarchy, obsCount, censusOverride)
        
        for filename in os.listdir("."):
            if filename == 'Incomplete-V3_' + targetCSV:
                try:
                    os.remove(filename[11:]) # del previous version if applicable
                except:
                    pass
                os.rename(filename, filename[11:])
                
        print ('V3 file created as: ' 'V3_' + targetCSV)
        print('')

        
        
