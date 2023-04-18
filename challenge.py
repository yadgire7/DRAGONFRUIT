import random
from pyspark import SparkContext, SparkConf



'''
- Each image is 100,000*100,000 pixels and is BLACK and WHITE (black:part of organism, whtie:surrounding)
- Considering the image to be a matrix of dimensions 100,000*100,000; with each pixel being a 0(white) or 1(black)(an interger)
  each location takes 4*3 = 12 bytes of memory (4 bytes each for row, column and value)
- Thus, the total memory required is 100,000*100,000*12 = 12,000,000,000 bytes = 12 GB
- Instead, we can use the information that only 25% of the image is black and the rest is white,
    and store the location of the black pixels only using the concept of sparse matrix
- Thus now, the total memory required is 100,000*100,000*0.25*8 = 2,000,000,000 bytes = 2 GB
- On an average, it wpuld take 30 GB of memory to store a 100,000*100,000 image (Google)
- So, this method will save 28 GB of memory per image
- This is the approach for task 1 part a. representing the black and white image

'''
# As the size of the dataset is huge to fit into the main memory, I am using pyspark to process the data 

# Considering we have 1000 images to be processed

# function to generate 1000 images
def generate_images(num_images):
    images = []
    for i in range(num_images):
        row = [random.choice([0,1]) for i in range(100000)]
        col = [random.choice([0,1]) for i in range(100000)]
        image = merge(row,col)
        images.append(image)
    return images

# function to represent black and white image as a sparse matrix


