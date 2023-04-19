import sys
import os
import random
from pyspark import SparkContext, SparkConf

# environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

'''
- Each image is 100,000*100,000 pixels and is BLACK and WHITE (black:part of organism, whtie:surrounding)
- Considering the image to be a matrix of dimensions 100,000*100,000; with each pixel being a 0(white) or 1(black)(an interger)
  each location takes 4*3 = 12 bytes of memory (4 bytes each for row, column and value)
- Thus, the total memory required is 100,000*100,000*12 = 12,000,000,000 bytes = 12 GB
- Instead, we can use the information that only 25% of the image is black and the rest is white,
    and store the location of the black pixels only using the concept of sparse matrix
- Thus now, the total memory required is 100,000*100,000*0.25*8 = 2,000,000,000 bytes = 2 GB
- On an average, it would take 30 GB of memory to store a 100,000*100,000 image (Google)
- So, this method will save 28 GB of memory per image
- This is the approach for task 1 part a. representing the black and white image

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
'''
# As the size of the dataset is huge to fit into the main memory, we can use spark (pyspark) to process the data parallely with many compute nodes

# function to generate image
def generate_fake_image():
    image = []
    for r in range(1000):
        # generate a random row
        row = [random.choice([0,1]) for i in range(100000)]
        # search for a black pixel in the row
        if 1 in row:
            r,c = r, row.index(1) # get the first black pixel
            zero_row = [0]*100000
            x = 50000
            #check if r + x and c + x are within the boundary
            if r + x > 100000:
                x = 100000 - r
            if c + x > 100000:
                x = 100000 - c
            # fill the square boundary with 1s
            for i in range(r, r+x):
                for j in range(c, c+x):
                    zero_row[j] = 1
                image.append(zero_row)
            # fill the remaining rows randomly
            for i in range(r+x, 100000):
                image.append([random.choice([0,1]) for i in range(100000)])
        else:
            image.append(row)
    return image



# function to store the image in a sparse matrix
'''
def store_image(img):
    sparse_img = []
    for i in range(len(img)):
        for j in range(len(img[i])):
            if img[i][j] == 1:
                sparse_img.append((i, j))
    return sparse_img
'''

data  = generate_images(2)
rdd = sc.parallelize(data)

img_details = rdd.map(lambda img: store_image(img))
print(img_details)










