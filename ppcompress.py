import os


def compressFile(file,compressionPath,compress):
    import gzip 
    import shutil 
    f_tail, f_head = os.path.split(file)
    
    compressedFilePath = os.path.join(compressionPath,f_head)
    compressedFilePath += ".gz"
      
    if compress:    
        f_in = open(file,"rb") 
        f_out = gzip.open(compressedFilePath,"wb")


        shutil.copyfileobj(f_in,f_out)
        f_in.close()
        f_out.close()
    else:
        try:
            f_in = gzip.open(compressionPath,"rb")
            data = f_in.read() # This should decompress the file 
            f_in.close()
            f_out = open(compressionPath[:-3],"w") # Remove the .gz suffix
            f_out.write(data)
            f_out.close() 
            
        except Exception as e:
            print(f"Failed to open {compressionPath} for decompression: {e}")
            return

    return 

def compressFilesInParallel(files,compressionPath,maxThreads,compress=True):
    import gzip
    from functools import partial 
    from concurrent.futures import ThreadPoolExecutor
    from itertools import repeat
    # files: List of absolute file paths 
    # CompressionPath, the path where the head of each file in files will be compressed.
    
    with ThreadPoolExecutor(max_workers=maxThreads) as ex:
        ex.map(compressFile,files,repeat(compressionPath),repeat(compress))
    return 


def processPath(path="",compressionPath="",maxThreads=1,compress=True):
    if path == "":
        path = os.getcwd()
    
    if compressionPath == "":
        _, pathHead = os.path.split(path)
        compressionPath = os.path.join(path,pathHead + ".ppc")
        
        os.makedirs(compressionPath,exist_ok=True)


    dirContents = os.listdir(path)

    dirs = [] # Directory and file stacks 
    files = []
    
    it = os.scandir(path)
    for entry in it:
        fullPath = os.path.join(path,entry)
        if os.path.isfile(fullPath):
            files.append(fullPath)
        elif os.path.isdir(fullPath):
            dirs.append(fullPath)
    
    compressFilesInParallel(files,compressionPath,maxThreads,compress=compress) 

    
    

    for dir in dirs:
        if dir == compressionPath:
            continue
        pathBase, pathHead = os.path.split(dir)
        newCompressionPath = os.path.join(compressionPath,pathHead)

        try:
            os.makedirs(newCompressionPath,exist_ok=True)
        except Exception as e:
            print(f"Failed to create directory {newCompressionPath}: {e}")

        processPath(path=dir,compressionPath=newCompressionPath,maxThreads=maxThreads)

if __name__ == "__main__":
    import sys 

    # Parse command line arguments 
    argc = len(sys.argv)
    maxThreads = 1
    rootPath = ""
    outputPath = ""
    compress = True 
    if argc == 0:
        sys.exit("Error: use -h for help.")
    for i,arg in enumerate(sys.argv):
        if i == 0:
            continue
        index = i + 1
        if arg == "-j":
            if argc < index + 1:
                sys.exit("Error: No argument for -j command option")
            maxThreads = int(sys.argv[i+1])
            if maxThreads <= 0:
                sys.exit("Error: Max Threads must be greater than 0")
        elif arg == "-o":
            if argc < index + 1:
                sys.exit("Error: No argument for -o command option")
            outputPath = sys.argv[i+1]
        elif arg == "-rootPath":
            if argc < index + 1:
                sys.exit("Error: No argument for -rootPath command option")
            rootPath = os.path.abspath(sys.argv[i+1])
        elif arg == "-c":
            compress=True
        elif arg == "-d":
            compress=False 
        elif arg == "-h":
            sys.exit(f"""Options:\n -j: Number of threads for parallel compression\n -rootPath: The path from which recursive compression should start
                    \n -o: The output directory for the recursive compression.
                    \n -c: Compress (default)
                    \n -d: Decompress 
                    """)
        elif sys.argv[i-1] not in ["-h","-j","-rootPath","-o"]:
            sys.exit(f"Error: Unrecognised command line argument {arg}")
    if outputPath != "":
        os.makedirs(outputPath,exist_ok=True)        
    processPath(path=rootPath,compressionPath=outputPath,maxThreads=maxThreads,compress=compress)

