import cv2

def dhash(image_path, hashSize=8):
    image = cv2.imread(image_path)
    # print ("Image -> {}".format(image))
    if image is None:
        return 0
    image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # print ("BW Image -> {}".format(image))
    resized = cv2.resize(image, (hashSize + 1, hashSize))
    diff = resized[:, 1:] > resized[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])