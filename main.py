import asyncio
import logging
import aioredis
import uvicorn
from sklearn.neighbors import NearestCentroid
from aioredis.client import PubSub, Redis
from sklearn.cluster import KMeans,AgglomerativeClustering
from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException, Header,Response,Request
from fastapi.responses import JSONResponse
from sqlalchemy.future import select
from sqlalchemy import create_engine
import _pickle as pickle
import redis
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import json
import schema
import models
import jwt
import aio_pika
import base64
from datetime import datetime,timedelta,timezone
import pytz
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
import os
import numpy as np
from celery import Celery
from dotenv import load_dotenv,find_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from secureapi import SecureAPIMiddleware,get_header
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv(find_dotenv())
engine = create_async_engine(
            os.environ['DATABASE_URI'],
            future=True,
            echo=True
        )

SessionLocal = sessionmaker(bind= engine ,expire_on_commit=False, class_=AsyncSession)
rediscommandsubs='EmbeddingCommandQueue'
pool=aioredis.ConnectionPool.from_url(f'redis://127.0.0.1:6379/0', encoding="utf-8", decode_responses=True)
mediods = {}
facethreshold=float(os.environ['facethreshold'])
maxcluster = int(os.environ['maxcluster'])
maxbatch = int(os.environ['maxbatch'])
minbatch = int(os.environ['minbatch'])

celery = Celery(
    __name__,
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0"
)

def db_engine():
    try:
        db = SessionLocal()
        yield db
    finally:
         db.close()

app = FastAPI()
app.add_middleware(SecureAPIMiddleware, some_attribute="some_attribute_here_if_needed")

@app.on_event("startup")
async def onstart():
    async def producer_handler(pubsub: PubSub,subscription,conn):
        await pubsub.subscribe(subscription)
        # assert isinstance(channel, PubSub)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message:
              try:
                data = json.loads(message['data'])
                if data['op']=='CREATE':
                    pipe = await conn.pipeline()
                    await pipe.rpush('embedding:' + data['part'], *data['embedding'])
                    await pipe.rpush('id:' + data['part'], str(data['id']) + ':' + data['processIdentifier'])
                    await pipe.execute()
                if data['op']=='DELETE':
                    #to implement in redis lua
                    l=conn.lpos('id:' + data['part'], str(data['id']) + ':' + data['processIdentifier'])
                    k=conn.lrange('embedding:'+ data['part'],0,-1)
                    del(k[l*data['size']:(l+1)*data['size']])
                    pipe = await conn.pipeline()
                    pipe.delete('embedding:'+ data['part'])
                    pipe.rpush('embedding:'+ data['part'],*k)
                    pipe.lrem('id:'+ data['part'], str(data['id']) + ':' + data.processIdentifier)
                if data['op']=='CLUSTER':
                    if data['category']:
                       ClusterPartition.delay(data['category'],data['entity'])
                    else:
                        ClusterPartitionBatch.delay()
#                    pipe.ltrim('embedding:'+str(data['size'])+':'+ data['part'],l*data['size'],(l+1)*data['size'])
              except Exception as e:
                  print(e)
                  print(message.data)
                  pass

    for k in os.listdir('mediods'):
       file = open('mediods/'+k , 'rb')
       mediods[k.split('.')[0]] = pickle.load(file)
       file.close()
    conn=await aioredis.Redis(connection_pool=pool)
    pubsub = conn.pubsub()
    asyncio.create_task(producer_handler(pubsub=pubsub, subscription=rediscommandsubs,conn=conn))
    #load two lua redis command to find face and delete

@app.post("/entityface/category/add", response_model=schema.addCategory)
async def addCategory(data: schema.addCategory,request: Request, db: AsyncSession = Depends(db_engine)):
    headers = get_header(request)
    query = select(models.Category).where(models.Category.entity == headers['entity'])
    q=(await db.execute(query)).scalars().all()
#    q = await db.query(models.Category).filter(models.Category.entity == headers[3]).all()
    if not q:
       query = "CREATE TABLE if not exists " + 'ENT'+str(headers['entity']) + " PARTITION OF Face FOR VALUES IN ("+ str(headers['entity'])+")"
       await (await engine.connect()).execute(text(query))
       #tmp=(await db.execute(text(query)))

    temp1=True
    for q1 in q:
        if q1.name==data.name:
            temp1=False
            break
    if temp1:
           temp=[{"User": headers['user'],"ACTION":"ADD","TS": datetime.now().strftime('%m%d%y%H%M%S')}]
           db_user = models.Category(name=data.name,updateInfo=json.dumps(temp),entity=headers['entity'],model=data.model,embeddingLength=data.embeddingLength )
           db.add(db_user)
           await db.commit()
           await db.refresh(db_user)
           await db.close()
           return db_user
    else:
          return JSONResponse(status_code=501, content={'status': 'Already Exist'})

@app.post("/face/partition")
async def partitionFace(request: Request):
    category='test'
    body=json.loads(request.body())
    if body[category]=='*':
      status=ClusterPartitionBatch.delay()
    else:
      status=ClusterPartition.delay(body[category]=='*')
    return JSONResponse(status_code=204, content={'batchid': status})

@app.post("/entityface/face/find")
async def findFace(data: schema.findface,request: Request):
    headers = get_header(request)
    partition=str(headers['entity'])+data.category
    if partition in mediods:
       partition= partition + ':' +findpartition(mediods[partition],data.embedding)
    redist = await aioredis.Redis(connection_pool=pool)
    return await findMatch(partition,data,redist)


async def findMatch(partition,data:schema.findface,redist):
    t = len(data.embedding)
    embedding=await redist.lrange('embedding:'+partition, 0, -1)
    temp=schema.findfaceResult(matched=[],unmatched=[])
    id=await redist.lrange('id:'+partition, 0, -1)
    distance=[]
    thresindex=-1
    for i in range(len(id)):
        temp1 = [float(k) for k in embedding[i*t:(i+1)*t]]
        dist=findEuclideanDistanceArray(temp1,data.embedding)
        ins=False
        for j in range(len(distance)):
            if dist<distance[j][0]:
                distance.insert(j,(dist,id[i]))
                ins=True
                break
        if not ins:
            distance.append((dist, id[i]))
        if dist <= facethreshold:
            thresindex = thresindex + 1
    for i in range(data.maxFaces):
        if i<=thresindex:
            temp.matched.append(distance[i][1])
        else:
            temp.unmatched.append(distance[i][1])
    return temp

def findpartition(med,embedding):
    i=0 if findEuclideanDistanceArray(med[0][0],embedding)>findEuclideanDistanceArray(med[1][0],embedding) else 1
    category=''
    if med[i][2]:
        category=findpartition(med[i][2],embedding)
    return str(i)+':'+category

@app.post("/entityface/face/add")
async def addFace(data: schema.addFace,request: Request, db: Session = Depends(db_engine)):
    existing=False
    headers = get_header(request)
    if not len(data.embedding)/data.embedding_size==len(data.images):
        return JSONResponse(status_code=501, content={'reason': "invalid data"})
    try:
      query = select(models.Category).where(models.Category.name == data.category and models.Category.entity == headers['entity'])
      category = (await db.execute(query)).scalar()
    except:
        return JSONResponse(status_code=501, content={'reason': "invalid category"})
    if not (category.model==data.model and category.embeddingLength==data.embedding_size):
        return JSONResponse(status_code=501, content={'reason': "invalid category"})
    if data.id!=None:
       select(models.Face).where(models.Face.category_name == data.category and models.Face.id == data.id)
       q = await db.execute(query)
       if q:
           existing=True
    if not existing:
        q = models.Face(category_name=data.category,entity=headers['entity'],status = 'ADD',processIdentifier = data.processIdentifier)
        q.embedding = []
        q.images = []
        q.processInfo=[]
        q.updateInfo=[]
        q.redisPartitionKey=[]
    i=0
    for im in data.images:
       q.images.append(base64.b64decode(im.encode("utf-8")))
    q.embedding.extend(data.embedding)
    q.embedding_size=data.embedding_size
    q.processInfo.append(data.processInfo)
    q.updateInfo.append({"User": headers['user'],"ACTION":"ADD","TS": datetime.now().strftime('%m%d%y%H%M%S')})
    db.add(q)
    await db.commit()
    await db.refresh(q)
    for im in data.images:
       if data.realtime:
           partition = str(headers['entity']) + data.category
           if partition in mediods.keys():
               partition = partition + ':' + findpartition(mediods[partition], data.embedding[i*data.embedding_size:(i+1)*data.embedding_size])
           redist = await aioredis.Redis(connection_pool=pool)
           q.redisPartitionKey.append(str(len(q.images) - 1) + '#' + partition)
           await publishRedisCommand(redist, 'CREATE', q, partition)
           i=i+1
    return {"id":q.id}

async def publishRedisCommand(redist,op,data,partition):
    cmdData={'op':op,'part':partition,'size':data.embedding_size,'processIdentifier':data.processIdentifier,'id':str(data.id)}
    if op=='CREATE':
        cmdData['embedding']=data.embedding
    await redist.publish(rediscommandsubs,json.dumps(cmdData))
    print("command published")
    return

def findEuclideanDistance(source_representation, test_representation):
    euclidean_distance = source_representation - test_representation
    euclidean_distance = np.sum(np.multiply(euclidean_distance, euclidean_distance))
    euclidean_distance = np.sqrt(euclidean_distance)
    return euclidean_distance

def findEuclideanDistanceArray(source_representation, test_representation):
    i=0;
    euclidean_distance=0
    for t in source_representation:
      temp=source_representation[i] - float(test_representation[i])
      euclidean_distance = euclidean_distance + temp*temp
      i=i+1
    return np.sqrt(euclidean_distance)

@celery.task
async def ClusterPartitionBatch():
    db = db_engine()
    category = await db.query(models.Category).all()
    print("cluster partition batch triggered")
    for cat in category:
        ClusterPartition.delay(cat.name,cat.entity)

@celery.task
async def ClusterPartition(cat,entity):
    db=db_engine()
    query = select(models.Category).where(models.Category.entity == entity and models.Category.name == cat)
    category = (await db.execute(query)).first()
    q = await db.query(models.Face).filter(models.Face.category_name == category.name and models.Face.status == 'ACTIVE' and models.Face.embedding_size == category.embedding_size and models.Face.entity==category.entity)
    cluster_name = str(category.entity)+category.name
    level = 0
    all_object = q.all()
    redist = redis.Redis(
        host='127.0.0.1',
        port=6379)
    for key in redist.scan_iter('roleextension*'):
        redist.delete(key)
    t = cluster_div(all_object, cluster_name, maxbatch, maxcluster, minbatch, redist)
    with open('mediods/' + category + '.pickle', 'wb') as handle:
        pickle.dump(t, handle)
    await db.commit()
    return 'ok'

def cluster_div(all_object,cluster_name,maxbatch,maxcluster,minbatch,redist):
  tt=[]
  mediods=[]
  for t1 in all_object:
    tt.extend( np.array_split(np.array(t1.embedding), len(t1.embedding) / t1.embedding_size))
  cluster=int(len(tt)/minbatch)
  embedding=np.array(tt)
  if cluster>maxcluster:
      cluster=maxcluster
#  KMobj = KMedoids(n_clusters=cluster).fit(embedding)
  KMobj = KMeans(n_clusters=cluster).fit(embedding)
  p0=np.count_nonzero(KMobj.labels_ == 0)
  p1=np.count_nonzero(KMobj.labels_ == 1)
  clusterlist=[[] for aa in range(cluster)]
  branchend=False
  aglo=False
  if( min(p0,p1)<.2*max(p0,p1) or min(p0,p1)<minbatch):
    temp = AgglomerativeClustering().fit(embedding)
#    print('l0:' + str(np.count_nonzero(KMobj.labels_ == 0)) + ',' + 'l1:' + str(np.count_nonzero(KMobj.labels_ == 1)))
    a0 = np.count_nonzero(temp.labels_ == 0)
    a1 = np.count_nonzero(temp.labels_ == 1)
    if(min(a0,a1)/max(a0,a1)>min(p0,p1)/max(p0,p1)):
      clusterpoint=NearestCentroid().fit(np.array(tt), temp.labels_).centroids_
      aglo=True
      p0=a0;p1=a1
    if(min(p0, p1) < minbatch):
      branchend=True
  if not aglo:
    clusterpoint = KMobj.cluster_centers_

  if not branchend:
    i=0
    for t1 in all_object:
      if aglo:
        aa=np.array_split(np.array(t1.embedding), len(t1.embedding) / t1.embedding_size)
        cost=np.zeros(cluster)
        for a in aa:
          cost=cost+[findEuclideanDistance(x,a) for x in clusterpoint]
        ind=np.argmin(cost)
        clusterlist[ind].append(t1)
      else:
        clusterlist[KMobj.labels_[i]].append(t1)
      i=i+1
    ind = 0
    for clusterp in clusterpoint:
      med=[clusterp]
      med.append(len(clusterlist[ind]))
      if len(clusterlist[ind])>maxbatch:
         mediod=cluster_div(clusterlist[ind],cluster_name+':'+str(ind),maxbatch,maxcluster,minbatch,redist)
         med.append(mediod)
         if not mediod:
           redis_embedding=[]
           cluster_id=[]
           for ob in clusterlist[ind]:
              ob.status = "LISTED"
              cluster_id.append(str(ob.id)+'::'+ob.processIdentifier)
              redis_embedding.extend(ob.embedding)
              ob.redisPartitionKey.append(cluster_name + ':' + str(ind))
           print(cluster_name + ':' + str(ind) + '=' + str(len(redis_object))+' branchend')
           redist.rpush('embedding:'+cluster_name + ':' + str(ind),*redis_embedding)
           redist.rpush('id:'+cluster_name + ':' + str(ind),*cluster_id)
           redis_object = {'id':cluster_id,'embedding':redis_embedding}
           filename=('backup/' + cluster_name + ':' + str(ind) + '.pickle').replace(':','&')
           with open(filename, 'wb') as handle:
               pickle.dump(redis_object, handle)
           if (len(redis_object) > maxbatch + minbatch) or len(redis_object) < minbatch:
              print(filename+':: clustering not within defined limit')
      else:
          redis_embedding = []
          cluster_id = []
          for ob in clusterlist[ind]:
             ob.status="LISTED"
             cluster_id.append(str(ob.id) + '::' + ob.processIdentifier)
             redis_embedding.extend(ob.embedding)
             ob.redisPartitionKey.append(cluster_name + ':' + str(ind))
          redist.rpush('embedding:'+str(clusterlist[ind][0].embedding_size)+':' + cluster_name + ':' + str(ind),*redis_embedding)
          redist.rpush('id:'+str(clusterlist[ind][0].embedding_size)+':' + cluster_name + ':' + str(ind),*cluster_id)
          filename = ('backup/' + cluster_name + ':' + str(ind) + '.pickle').replace(':', '&')
          redis_object = {'id': cluster_id, 'embedding': redis_embedding}
          with open(filename, 'wb') as handle:
              pickle.dump(redis_object, handle)
          med.append([])
          print(cluster_name+':'+str(ind)+'='+str(len(redis_object)))
      mediods.append(med)
      ind=ind+1
  return mediods
# To run locally
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)