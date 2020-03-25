# cosinesimilarity
Cosine similarity

This repository is for testing cosine similarity example
All sourcecode, and examples belong to me, this repository can only be viewed for learning purposes and should not be reprogrammed or reproduced for anything else.

Makefile is present in the src directory

Cosine Similarity example in c++

This software loads a text file or manual inputs from user and prepares it for computing similarity of an input with the loaded corpus. File loading is parallelized using OpenMP based on a double buffer strategy for faster loading.

Cosine similarity computation and matrix selection has been optimized and parallelized. 

Using bigrams and trigrams for similarity and quad grams for row selection. Not all quads are used and have been fine tuned by me by making a histogram.
