import os

for dirPath, dirNames, fileNames in os.walk("./"):
    if dirNames != 'bug': 
        print dirPath, dirNames
