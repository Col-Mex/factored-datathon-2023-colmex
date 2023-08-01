from extract import extract_stream
from transform import extract_stream
from load import loading


datos = extract_stream(id=20001)
filtrados = extract_stream(datos)
loading(filtrados)

# Pipeline
# Extract from stream data
# Return dataframe and filter and transform
# Load

