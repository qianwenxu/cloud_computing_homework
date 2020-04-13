import pymongo

def write_mongo(data):
    '''
    @params: data
    '''
    try:
        client = pymongo.MongoClient('localhost',27017)
        db = client.db
        table = db['douban_book']
        result = table.insert_one(data)
        # print result
        # print result.inserted_id
    except Exception as e:
        print e

def sample():

    file_title = open('group/title.txt','r')
    file_detail = open('group/detail.txt','r')

    line = file_title.readline()
    while line:
    	index = line.find(' ')
    	data_dict = {}
    	data_dict['id'] = int(line[0 : index])
        data_dict['title'] = line[index + 1 : len(line)]
        line2 = file_detail.readline()
        data_dict['detail'] = line2[line2.find(' ') + 1 : len(line2)]
        write_mongo(data_dict)
        # print line[0 : index]
        line = file_title.readline()

    file_title.close()
    file_detail.close()

if __name__ == '__main__':
    try:
        sample()
    except Exception as e:
        print e
