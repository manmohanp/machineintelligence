from imutils.video import VideoStream
import cv2 as cv
import sys
import numpy as np
import os.path

net = cv.dnn.readNetFromDarknet("model/yolov3.cfg", "model/yolov3.weights")
net.setPreferableBackend(cv.dnn.DNN_BACKEND_OPENCV)
net.setPreferableTarget(cv.dnn.DNN_TARGET_CPU)

# load names of classes of coco dataset
classesFile = "model/coco.names"
classes = None
with open(classesFile, 'rt') as f:
    classes = f.read().rstrip('\n').split('\n')

# threshold configuration
confidenceThreshold = 0.5
nmsThreshold = 0.4

# draw bounding box
def drawBoundingBox(classId, conf, left, top, right, bottom):
    # Draw a bounding box.
    cv.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 3)

    label = '%.2f' % conf

    # create a label with class name and its confidence
    if classes:
        label = '%s:%s' % (classes[classId], label)

    labelSize, baseLine = cv.getTextSize(label, cv.FONT_HERSHEY_SIMPLEX, 0.5, 1)
    top = max(top, labelSize[1])
    cv.rectangle(frame, (left, top - round(1.5*labelSize[1])), (left + round(1.5*labelSize[0]), top + baseLine), (0, 0, 0), cv.FILLED)
    cv.putText(frame, label, (left, top), cv.FONT_HERSHEY_SIMPLEX, 0.75, (0,255,0), 1)

def processOutput(frame, outs):
    frameHeight = frame.shape[0]
    frameWidth = frame.shape[1]

    classIds = []
    confidences = []
    boxes = []
    for out in outs:
        for detection in out:
            scores = detection[5:]
            classId = np.argmax(scores)
            confidence = scores[classId]
            if confidence > confidenceThreshold:
                center_x = int(detection[0] * frameWidth)
                center_y = int(detection[1] * frameHeight)
                width = int(detection[2] * frameWidth)
                height = int(detection[3] * frameHeight)
                left = int(center_x - width / 2)
                top = int(center_y - height / 2)
                classIds.append(classId)
                confidences.append(float(confidence))
                boxes.append([left, top, width, height])

    indices = cv.dnn.NMSBoxes(boxes, confidences, confidenceThreshold, nmsThreshold)
    for i in indices:
        i = i[0]
        box = boxes[i]
        left = box[0]
        top = box[1]
        width = box[2]
        height = box[3]
        drawBoundingBox(classIds[i], confidences[i], left, top, left + width, top + height)

def getLayerNames(net):
    layersNames = net.getLayerNames()
    return [layersNames[i[0] - 1] for i in net.getUnconnectedOutLayers()]

# Process inputs
windowName = 'Real-time object detection using Yolo'
cv.namedWindow(windowName, cv.WINDOW_NORMAL)

# video stream from camera
vs = VideoStream(src=0).start()
cv.resizeWindow(windowName, 800, 600)

inputWidth = 416
inputHeight = 416

while True:

    # grab the frame from video stream
    frame = vs.read()

    # create blob from frame.
    blob = cv.dnn.blobFromImage(frame, 1/255, (inputWidth, inputHeight), [0,0,0], 1, crop=False)

    # set input to the network
    net.setInput(blob)

    # forward pass to get output of the output layers
    outs = net.forward(getLayerNames(net))

    processOutput(frame, outs)

    cv.imshow(windowName, frame)
    key = cv.waitKey(1) & 0xFF

	# if the `q` key was pressed, break from the loop
    if key == ord("q"):
        break
