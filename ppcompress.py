import os


def compressFile(file,compressionPath):
    import gzip 
    import shutil 
    f_tail, f_head = os.path.split(file)
    
    compressedFilePath = os.path.join(compressionPath,f_head)
    compressedFilePath += ".gz"
      
    
    f_in = open(file,"rb") 
    f_out = gzip.open(compressedFilePath,"wb")


    shutil.copyfileobj(f_in,f_out)
    f_in.close()
    f_out.close()
    return 

def compressFilesInParallel(files,compressionPath,maxThreads):
    import gzip
    from functools import partial 
    from concurrent.futures import ThreadPoolExecutor
    from itertools import repeat
    # files: List of absolute file paths 
    # CompressionPath, the path where the head of each file in files will be compressed.
    
    with ThreadPoolExecutor(max_workers=maxThreads) as ex:
        ex.map(compressFile,files,repeat(compressionPath))
    return 

def decompressFile(path,outputPath):
    import gzip 
    try:
        assert path.endswith(".gz")

        tail, head = os.path.split(path)
        f_out_path = os.path.join(outputPath,head[:-3]) # remove the .gz 
        f_in = gzip.open(path,"rb")
        f_out = open(f_out_path,"wb")

        data = f_in.read()
        f_out.write(data)
        f_in.close()
        f_out.close()
        return
    except Exception as e:
        print(f"Failed to decompress {path}: {e}")


def decompressFilesInParallel(files,maxThreads,decompressionPath):
    from concurrent.futures import ThreadPoolExecutor
    from itertools import repeat 
    
    tp = ThreadPoolExecutor(max_workers=maxThreads)
    tp.map(decompressFile,files,repeat(decompressionPath))
    return 

def processPath(path="",compressionPath="",maxThreads=1,compress=True):
    if path == "":
        path = os.getcwd()
    
    if compressionPath == "" and compress:
        _, pathHead = os.path.split(path)
        compressionPath = os.path.join(path,pathHead + ".ppc")
        os.makedirs(compressionPath,exist_ok=True)

    elif compressionPath == "" and not compress:
        compressionPath = path + ".ppd" 

    

    dirs = [] # Directory and file stacks 
    files = []
    if compress:
        it = os.scandir(path)
        for entry in it:
            fullPath = os.path.join(path,entry)
            if os.path.isfile(fullPath):
                files.append(fullPath)
            elif os.path.isdir(fullPath):
                dirs.append(fullPath)
    
        compressFilesInParallel(files,compressionPath,maxThreads) 
    else:
        # rootpath becomes the path to the root file to decompress
        if os.path.isfile(path) and path.endswith(".gz"):
            decompressFile(path,compressionPath)

        elif os.path.isdir(path):
            it = os.scandir(path)
            for entry in it:
                if os.path.isfile(entry):
                    if entry.name.endswith(".gz"):
                        files.append(entry.path)
                elif os.path.isdir(entry):
                    dirs.append(entry.path)
        decompressFilesInParallel(files,maxThreads,compressionPath)    
    for dir in dirs:
        if compress:
            if dir == compressionPath:
                continue
            _, pathHead = os.path.split(dir)
            newCompressionPath = os.path.join(compressionPath,pathHead)

            try:
                os.makedirs(newCompressionPath,exist_ok=True)
            except Exception as e:
                print(f"Failed to create directory {newCompressionPath}: {e}")

            processPath(path=dir,compressionPath=newCompressionPath,maxThreads=maxThreads,compress=compress)
        else:
            # Decompress the files then recurse through directories
            if dir == compressionPath:
                continue
            _, pathHead = os.path.split(dir)
            newDecompressionpath = os.path.join(compressionPath,pathHead)
            try:
                os.makedirs(newDecompressionpath,exist_ok=True)
            except Exception as e:
                print(f"Failed to create directory {newDecompressionpath}: {e}")
            processPath(path=dir,compressionPath=newDecompressionpath,maxThreads=maxThreads,compress=compress)
    return

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
            sys.exit(f"""Options:
                    -j: Number of threads for parallel compression
                    -rootPath: The path from which recursive compression should start
                    -o: The output directory for the recursive compression.
                    -c: Compress (default)
                    -d: Decompress, this changes the behaviour of -rootPath to be the path to the root file decompress and -o the root directory to decompress into.
                    """)
        elif sys.argv[i-1] not in ["-h","-j","-rootPath","-o"]:
            sys.exit(f"Error: Unrecognised command line argument {arg}")
    if outputPath != "":
        os.makedirs(outputPath,exist_ok=True)        
    processPath(path=rootPath,compressionPath=outputPath,maxThreads=maxThreads,compress=compress)

