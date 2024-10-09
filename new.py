import torch
import numpy as np
import torchvision.transforms as transforms

from pymilvus import(
    Milvus,
    IndexType,
    Status,
    connections,
    FieldSchema,
    DataType,
    Collection,
    CollectionSchema,
    utility,
    MilvusClient
)

from astropy.io import fits
from skimage.transform import resize
from torchvision.models import resnet50

def connect_to_milvus() :
    
    host         = 'eu-de.services.cloud.techzone.ibm.com'
    port         = 21400
    user         = 'ibmlhadmin'
    key          = 'password'
    server_pem_path = 'presto.crt'
    uri="http://" +  host + ":" + str(port) 

    client = MilvusClient(
                       uri=uri, 
                       user=user,
                       password=key,
                       server_pem_path=server_pem_path,
                       server_name='watsonxdata',
                       secure=True)  
    return client

def create_collection():
    
    utility.drop_collection("image_embeddings")
    
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1000)       
    ]
    schema = CollectionSchema(fields, "Embedding of FITS image file")
    
    fits_coll = Collection("image_embeddings", schema)

    index_params = {
            'metric_type':'L2',
            'index_type':"IVF_FLAT",
            'params':{"nlist":2048}
    }
    fits_coll.create_index(field_name="embedding", index_params=index_params)

    fits_coll.flush()
    
    return(fits_coll)
    
def load_fits_file(file_path) :
    
    with fits.open(file_path) as hdul:
   
        image_data = hdul[0].data
        image_data = (image_data - np.min(image_data)) / (np.max(image_data) - np.min(image_data))
        image_resized = resize(image_data, (224, 224), mode='reflect')
        if len(image_resized.shape) == 2:  
            image_resized = np.stack([image_resized] * 3, axis=-1)  

    return (image_resized ) 
    
def generate_embedding(image) :

    # Load pre-trained ResNet model
    model = resnet50()
    model.eval()  # Set the model to evaluation mode
    
    # Convert the image to a tensor
    preprocess = transforms.Compose([
        transforms.ToPILImage(),
        #transforms.Resize((224, 224)),
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    image_tensor = preprocess(image)
    image_tensor = image_tensor.unsqueeze(0)  # Add batch dimension
    
    # Generate the embedding (using the model without the classification layer)
    with torch.no_grad():
        embedding = model(image_tensor)

    return embedding.squeeze().numpy().astype(np.float32)
    

def insert_embedding(fits_coll, embedding):
    fits_coll.insert([[embedding]])
    fits_coll.load()

client = connect_to_milvus()

# client.list_users()

client.close()

exit()

fits_coll = create_collection(client)
image_data = load_fits_file("m31.fits")
embedding_vector = generate_embedding(image_data)
insert_embedding(fits_coll, embedding_vector)