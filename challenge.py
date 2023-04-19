'''
Author: Saurabh Arun Yadgire
GitHub: yadgire7
'''
import sys
import os
import random
import math
from pyspark import SparkContext, SparkConf
import timeit
# environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


#################################################
# child functions for dye_image
'''
function to get the 8 neighbors of a pixel/ cell
'''
#################################################

def get_neighbors(image, row, col):
    neighbors = []
    for i in range(row-1, row+2):
        for j in range(col-1, col+2):
            if i >= 0 and i < len(image) and j >= 0 and j < len(image[0]):
                neighbors.append((i, j))
    neighbors.remove((row, col))
    return neighbors


###############################################
'''
Randomly choose whether to dye the pixel or not
i.e whether the cell is cancer-causing or not
if chosen to dye:
    select a random number of neighbors to die         
'''
################################################


def generate_dye_image(dye_image, r, c, x):
    for i in range(r, r+x):
        for j in range(c, c+x):
            choose = random.choice([True, False])
            if choose == True:
                neighbors = get_neighbors(dye_image, r, c)
                k = random.randint(0, len(neighbors))
                sample = random.sample(neighbors, k)
                for pos in sample:
                    dye_image[pos[0]][pos[1]] = 1
                dye_image[i][j] = 1
    return dye_image


#########################################################################
'''
generate microscopic image(black and white) and dyed image simultaneously
1. choose the area covered by parasite randomly between (0.25,0.70)
2. fill a square region(assumption explained in readme)
that covers above chosen area of the image
3. dye the image using above functions to dye the image
4. get both images simulataneously
'''
#########################################################################


def generate_fake_image(size):
    image = []
    dye_image = [[0]*size for i in range(size)]
    pick = round(random.uniform(0.25, 0.70), 2)
    x = int(math.sqrt(int(pick*(size**2))))
    for r in range(size):
        # generate a random row
        row = [random.choice([0, 1]) for i in range(size)]
        # search for a black pixel in the row
        if 1 in row:
            r, c = r, row.index(1)  # get the first black pixel
            zero_row = [0]*size
            # check if r + x and c + x are within the boundary
            if r + x > size:
                x = size - r
            if c + x > size:
                x = size - c
            # fill the square boundary with 1s
            for i in range(r, r+x):
                for j in range(c, c+x):
                    zero_row[j] = 1
            for i in range(r, r+x):
                image.append(zero_row)
            # generate dye_image simultaneously
            dye_image = generate_dye_image(dye_image, r, c, x)
            # fill the remaining rows with 0s
            for i in range(r+x, size):
                image.append([0]*size)
            break
        else:
            image.append(row)
    return image, dye_image


####################################################################
'''
function to decide whether parasite is infected with cancer or not
function to calulate area covered
Solution to task 4:
Use pyspark to computer results parallely and improve the processing speed
'''
####################################################################


def solution(image_rdd, dye_rdd):
    area_image = image_rdd.map(lambda row: (1, sum(row)))\
        .reduceByKey(lambda a, b: a+b).map(lambda area: area[1]).first()

    area_dye = dye_rdd.map(lambda row: (1, sum(row)))\
        .reduceByKey(lambda a, b: a+b).map(lambda area: area[1]).first()

    if area_dye/area_image >= 0.1:
        return "Parasite is infected with cancer."
    else:
        return "Parasite is  NOT infected with cancer."

####################################################################

if __name__ == '__main__':
    sc = SparkContext().getOrCreate()
    # generate images
    image, dye = generate_fake_image(100000)

    # parallelize data
    image_rdd = sc.parallelize(image)
    dye_rdd = sc.parallelize(dye)

    # calculate area using mapreduce
    start = timeit.default_timer()
    decision = solution(image_rdd, dye_rdd)
    end = timeit.default_timer()
    print(decision)
    print(f"Time taken: {end-start} seconds\n")

# results for size = 10000 on Google Colab
'''
Parasite is infected with cancer.
Time taken: 10.40736301100003 seconds
'''