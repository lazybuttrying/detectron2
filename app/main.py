from typing import Optional, List
from fastapi import FastAPI, File, UploadFile
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from datetime import datetime
import csv
from requests import request
import shutil
import subprocess
import os
import shutil
import boto3
import uvicorn
from mangum import Mangum

class Grape(BaseModel):
    filename: str
    berry: int

class GrapeList(BaseModel):
    data: List[Grape]

app = FastAPI()


bucket = 'afarm7defa4de003f406bbc7d6b4bdadbe11820155-dev'

url = "https://better-rat-41.hasura.app/v1/graphql"
payload="{\"query\":\"mutation MyMutation {\\n  insert_afarm_grape_one(object: {grape_id: %s, quality_id: %s, img: \\\"%s\\\"}) {\\n    grape_id\\n  }\\n}\\n\",\"variables\":{}}"
headers = {
        "Content-Type":"application/json",
        "x-hasura-admin-secret":"i7IqYmNWceH9bKQAtqMy5hIdujfvMQCeKjJf2JadYfFbhvXug2xatLayZB0HDFLA"
        }

bucket = 'afarm7defa4de003f406bbc7d6b4bdadbe11820155-dev'
client_s3 = boto3.client(
        's3',
        aws_access_key_id = 'AKIATT5FWZ7H7T527ZU6',
        aws_secret_access_key = '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',
)

def upload_file(file, key):
    global bucket
    try:
        client_s3.upload_file(
                file,
                bucket,
                key,
        )
    except Exception as e:
        print(f"Another Error => {e}")



@app.get("/") # for checking is server live
def read_root():
    return {"Hello": "World"}

@app.post("/count-berries")
async def count_berries(quality_id: int, images: List[UploadFile]=File(...)):
    src_path = './app/etc/'+str(quality_id)+"/"
    dest_path = './viz/'+str(quality_id)+"/"
    os.makedirs(src_path, exist_ok = True)
    os.makedirs(dest_path, exist_ok = True)
    path = []
    for image in images:
        name, ext = os.path.splitext(image.filename)
        path.append(name+"_"+str(datetime.now()).replace(' ','_')+"_"+ext)
        with open(src_path+path[-1], 'wb') as fileimg:
            fileimg.write(image.file.read())
    
    args = ['python3.9',"./demo/demo.py", "--input"]
    for p in list(map(lambda x:src_path+x, path)):
        args.append(p)
    for x in [ "--code", str(quality_id), "--output", dest_path,
        "--opts", "MODEL.DEVICE", "cpu",
        "MODEL.WEIGHTS", "./training_dir/BoxInst_MS_R_50_1x/model_final.pth"]:
        args.append(x)
    test = subprocess.run(args, capture_output=True)
    
    #print('err',test.stderr)
    #print('out',test.stdout)
    result_csv = csv.reader(open('./viz/results/'+str(quality_id)+'.csv'))
    
    while (len(os.listdir(src_path)) > len(os.listdir(dest_path))):
        pass # wait until all result file created

    grape_list = []
    grape_id = 1
    for p in path:
        response = request("POST", url, headers=headers, 
                data = payload % (quality_id, grape_id, p))
        while "errors" in response:
            grape_id += 1
            response = request("POST", url, headers=headers, 
                data = payload % (quality_id, grape_id, p))
        
        upload_file(file=dest_path+p, key="result/"+p)
        obj = Grape(filename=p, berry=int(next(result_csv)[1]))
        grape_list.append(obj)
    
    shutil.rmtree(src_path)
    shutil.rmtree(dest_path)

    return GrapeList(data = grape_list).dict()

handler = Mangum(app)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
