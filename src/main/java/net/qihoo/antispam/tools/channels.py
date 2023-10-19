
import os
def get_excluded_channels():
    ret = []
    file =open(r"/tmp/sslm/model/exclude.txt")
    line = file.readline()
    while line:
        ret.append(line.strip())
        line = file.readline()
    file.close()
    return ret
