import pymongo

def write_mongo(data):
    try:
        client = pymongo.MongoClient('localhost',27017)
        db = client.db
        table = db['GraphX']
        result = table.insert_one(data)
    except Exception as e:
        print e

def sample():
    inputFile = open('/Users/wn/Project/graphx/data/input.txt', 'r')
    curId = ""
    for line in inputFile:
        if len(line) >= 6 and line[1:6] == 'index':
            curId = line[6:len(line) - 1]
        if len(line) >= 2 and line[1] == '%':
            relationship = {}
            relationship["src"] = curId
            relationship["des"] = line[2: len(line)]
            write_mongo(relationship)
    inputFile.close()
    outputFile = open('/Users/wn/Project/graphx/data/output.txt', 'w')
    for r in relationships:
        outputFile.write(r)
    outputFile.close()

if __name__ == '__main__':
    try:
        sample()
    except Exception as e:
        print e
