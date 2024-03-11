import io
import base64
import keras
from PIL import Image
from keras.applications.resnet50 import ResNet50
from keras.applications.resnet50 import preprocess_input, decode_predictions
import numpy as np

class Classifer:
    def __init__(self) -> None:
        self.model = ResNet50(weights='imagenet')
        self.model.summary()

    def predict(self, img_str) -> list:
        #img = Image.open(io.BytesIO(base64.b64decode(img_str)))
        img = keras.utils.load_img(io.BytesIO(base64.b64decode(img_str)), target_size=(224, 224))
        img_arr = keras.utils.img_to_array(img)
        img_arr = np.expand_dims(img_arr, axis=0)
        img_arr = preprocess_input(img_arr)
        preds = self.model.predict(img_arr)
        return decode_predictions(preds, top=3)[0]