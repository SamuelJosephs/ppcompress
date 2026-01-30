# ppcompress
A python script for compressing a large amount of files in parallel using only the python standard library.

The command `python3 ppcompress.py -rootPath ./tests -o test-ppc-2 -j 10` will recursively compress ./tests into the output directory test-ppc-2 using a maximum of 10 threads. `python3 ppcompress.py -rootPath ./compressed-dir -o test-decompressed -j 10 -d` will decompress compressed-dir into test-decompressed with 10 threads.

 Add the following to your .bashrc for convinient usage:

 ```
function ppcompress(){
        module load python
        python3 ~/ppcompress/ppcompress.py "$@"
}
export -f ppcompress
```
