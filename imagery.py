import base64

imagery_schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Imagery",
      "description": "",
      "type": "object",
      "properties": {
        "title": {
          "description": "Image title",
          "type": "string"
        },
        "image_base64": {
          "description": "Base64 encoded image string",
          "type": "string"
        },
        "description": {
          "description": "Image description",
          "type": "string"
        },
        "classifications": {
          "description": "AI generated classifications",
          "type": "array",
          "minItems": 0,
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string"
              },
              "weight": {
                "type": "string"
              }
            },
            "required": [ "name", "weight" ]
          }
        }
      },
      "required": [ "title", "image_base64", "description" ]
    }
    """

class Classification:
    def __init__(self, name, weight) -> None:
        self.name = name
        self.weight = weight

class Imagery:
    def __init__(self, title, description) -> None:
        self.title = title
        self.description = description
        self.image_base64 = ""
        self.classifications = []

    def set_image(self, img_path):
        with open(img_path, "rb") as image_file:
            self.title = image_file.name
            self.description = img_path
            self.image_base64 = base64.b64encode(image_file.read()).decode()

    def add_classification(self, name, weight):
        self.classifications.append(Classification(name, weight))

def classification_to_dict(cls) -> dict:
    return dict(name=cls.name,
                weight=str(cls.weight))

def imagery_to_dict(imagery, ctx) -> dict:
    clss = map(classification_to_dict, imagery.classifications)
    return dict(title=imagery.title,
                description=imagery.description,
                image_base64=imagery.image_base64,
                classifications=list(clss))

def dict_to_imagery(obj, ctx) -> Imagery:
    img = Imagery(obj['title'], obj['description'])
    img.image_base64 = obj['image_base64']
    img.classifications = []
    return img