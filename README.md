# ppcompress
A python script for compressing a large amoung of files in parallel using only the python standard library.

The command `python3 ppcompress.py -rootPath ./tests -o test-ppc-2 -j 10` will recursively compress ./tests into the output directory test-ppc-2 using a maximum of 10 threads.
