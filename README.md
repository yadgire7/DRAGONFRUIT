# DRAGONFRUIT
CODING CHALLENGE

*Approach for task 1*
- Each image is 100,000*100,000 pixels and is BLACK and WHITE (black:part of organism, whtie:surrounding)
- Considering the image to be a matrix of dimensions 100,000*100,000; with each pixel being a 0(white) or 1(black)(an interger)
  each location takes 4*3 = 12 bytes of memory (4 bytes each for row, column and value)
- Thus, the total memory required is 100,000*100,000*12 = 12,000,000,000 bytes = 12 GB
- Instead, we can use the information that only 25% of the image is black(best case) and the rest is white,
    and store the location of the black pixels only using the concept of sparse matrix
- Thus now, the total memory required is 100,000*100,000*0.25*8 = 2,000,000,000 bytes = 2 GB
- On an average, it would take 30 GB of memory to store a 100,000*100,000 image (Google)
- So, this method will save 28 GB of memory per image
- This is the approach for task 1 part a. representing the black and white image
- In the worst case scenario, the image would take upto 12GB of memory

Time complexity:
- As the black portion(1s) is contiguous, we can assume that organism is contained in a square boundary,
  which occupies 25% of the image
- When we traverse the matrix and get our first black pixel, we can assume it as the first coordinate of the square boundary(r,c)
  and thus calculate the other 3 corners of the square boundary as (r, c+x), (r+x, c) and (r+x, c+x)
  such that x is the length of the square boundary
  implies x^2 = 25% of the image = 25*10^8 pixels
  implies x = 5*10^4 pixels
- Thus, the time required to store the actual image is the time required to traverse the matrix and get the first black pixel

Definitely, this approach is meant to give the best results in terms of memory and time complexity
at the cost of accuracy

*** As the size of the dataset is huge to fit into the main memory, we can use spark (pyspark) to process the data parallely with many compute nodes

*Approach for task 2*

For generating the image using the approach and data structures of task 1, we also need to consider that the black portion of the image will be contiguous, and thus we can assume that the organism is contained in a square boundary, which occupies 25% of the image.
We will generate randomly as a matrix in the following manner:
- generate each row aa seqewnce of 0s and 1s
- if the bit is 1, means the pixel is black.
- As soon as we get this first black pixel, we can assume it as the first coordinate of the square boundary(r,c) and thus calculate the other 3 corners of the square boundary as (r, c+x), (r+x, c) and (r+x, c+x) such that x is the length of the square boundary;
implies x^2 = 30% of the image = 3*10^7 pixels 
implies x = sqrt(3*10^7) ~ 5*10^3 pixels
- fill the rest of the maxtrix randomly with 0s and 1s
(Thus, the time required to generate the image is the time required to get the first black pixel(1))