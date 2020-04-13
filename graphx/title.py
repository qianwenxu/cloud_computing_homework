def sample():
    rankFile = open('/Users/wn/Project/graphx/data/rank_56/part-00000', 'r')
    rankList = dict()
    for line in rankFile:
        rankList[line.split(' ')[0]] = ''
    rankFile.close()

    inputFile = open('/Users/wn/Downloads/input.txt', 'r')
    curTitle = ''
    for line in inputFile:
        if len(line) >= 2 and line[1] == '*':
            curTitle = line[2: len(line) - 1]
        if len(line) >= 6 and line[1:6] == 'index':
            curId = line[6:len(line) - 1]
            if rankList.has_key(curId):
                rankList[curId] = curTitle
    inputFile.close()

    rankFile = open('/Users/wn/Project/graphx/data/rank_56/part-00000', 'r')
    titleFile = open('/Users/wn/Project/graphx/data/title_56.txt', 'w')
    for line in rankFile:
        appendLine = rankList[line.split(' ')[0]]
        line = line[0 : len(line) - 1] + ' ' + appendLine + '\n'
        titleFile.write(line)
    rankFile.close()
    titleFile.close();

if __name__ == '__main__':
    try:
        sample()
    except Exception as e:
        print e
