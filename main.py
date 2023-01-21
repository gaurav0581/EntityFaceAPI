import asyncio
import logging
import aioredis
import uvicorn
from sklearn.neighbors import NearestCentroid
from aioredis.client import PubSub, Redis
from sklearn.cluster import KMeans,AgglomerativeClustering
from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException, Header,Response,Request
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine
import _pickle as pickle
import redis
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
#engine = create_engine(os.environ['DATABASE_URI'])
SessionLocal = sessionmaker(bind= engine ,expire_on_commit=False, class_=AsyncSession)
rediscommandsubs='EmbeddingCommandQueue'
pool=aioredis.ConnectionPool.from_url(f'redis://127.0.0.1:6379/0', encoding="utf-8", decode_responses=True)
mediods = {}
facethreshold=7

def db_connect():
    return engine.connect()

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
#app.add_middleware(SecureAPIMiddleware, some_attribute="some_attribute_here_if_needed")

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
                if data['OP']=='CREATE':
                    pipe = await conn.pipeline()
                    pipe.lpush('embedding:' + data['part'], *data.embedding)
                    pipe.lpush('id:' + data['part'], str(data['id']) + ':' + data.processIdentifier)
                    pipe.execute()
                if data['OP']=='DELETE':
                    #to implement in redis lua
                    l=conn.lpos('id:' + data['part'], str(data['id']) + ':' + data.processIdentifier)
                    k=conn.lrange('embedding:'+ data['part'],0,-1)
                    del(k[l*data['size']:(l+1)*data['size']])
                    pipe = await conn.pipeline()
                    pipe.delete('embedding:'+ data['part'])
                    pipe.rpush('embedding:'+ data['part'],*k)
                    pipe.lrem('id:'+ data['part'], str(data['id']) + ':' + data.processIdentifier)
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
async def addCategory(data: schema.addCategory,request: Request, db: Session = Depends(db_engine),conn=Depends(db_connect)):
    headers = get_header(request)
    q = await db.query(models.Category).filter(models.Category.entity == headers[3]).all()
    if not q:
       query = "CREATE TABLE " + str(headers[3]) + " PARTITION OF Face FOR VALUES IN ('" + str(headers[3]) + "')"
       await conn.execute(query)
    else:
      temp1=True
      for q1 in q:
        if q1.name==data.name:
            temp1=False
            break
      if temp1:
           temp=[{"User": headers[0],"BUSINESS": headers[2],"ACTION":"ADD","TS": datetime.datetime.now().strftime('%m%d%y%H%M%S')}]
           db_user = models.Category(name=data.name,updateInfo=json.dumps(temp),entity=str(headers[3]),model=data.model,embeddingLength=data.embeddingLength )
           db.add(db_user)
           await db.commit()
           await db.refresh(db_user)
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


@app.post("/face/find")
async def findFace(data: schema.findface,request: Request, db: Session = Depends(db_engine)):
    headers = get_header(request)
    partition= headers[3] + ':' +findpartition(mediods[headers[3]],data.embedding)
    redist = await aioredis.Redis(connection_pool=pool)
    return await findMatch(partition,data,redist)

async def findMatch(partition,data:schema.findface,redist):
    t = len(data.embedding)
    embedding=redist.lrange('embedding:' +partition, 0, -1)
    temp=schema.findfaceResult(matched=[],unmatched=[])
    id=redist.lrange('id:'+partition, 0, -1)
    distance=[]
    thresindex=-1
    for i in range(len(id)):
        dist=findEuclideanDistanceArray(embedding[i*t:(i+1)*t],data.embedding)
        ins=False
        for j in range[len(distance)]:
            if dist<distance[j][0]:
                distance.insert(j,(dist,id[i]))
                ins=True
                break
        if ins:
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

@app.post("/face/add")
async def addFace(data: schema.addFace,request: Request, db: Session = Depends(db_engine)):
    existing=False
    headers = get_header(request)
    if not len(data.embedding)/data.embedding_size==len(data.images):
        return JSONResponse(status_code=501, content={'reason': "invalid data"})
    try:
      category = await db.query(models.Category).get(headers[3])
    except:
        return JSONResponse(status_code=501, content={'reason': "invalid category"})
    if data.id!=None:
       q = await db.query(models.Face).filter(models.Face.category_name==headers[3] and models.Face.id == data.id ).first()
       if q:
           existing=True
    if not existing:
        q = models.Face(category_name=headers[3],status = 'ADD',processIdentifier = data.processIdentifier)
        q.embedding = []
        q.images = []
        q.processInfo=[]
        q.updateInfo=[]
    for im in data.images:
       q.images.append(base64.b64decode(im.encode("utf-8")))
    q.embedding.extend(data.embedding)
    q.embedding_size=data.embedding_size
    q.processInfo.append(data.processInfo)
    q.updateInfo.append({"User": headers[0],"BUSINESS": headers[2],"ACTION":"ADD","TS": datetime.datetime.now().strftime('%m%d%y%H%M%S')})
    db.add(q)
    await db.commit()
    await db.refresh(q)
    if data.realtime:
        partition = headers[3] + ':' + findpartition(mediods[headers[3]], data.embedding)
        redist = await aioredis.Redis(connection_pool=pool)
        publishRedisCommand(redist, 'create', data, partition)
    return {"id":q.id}

def publishRedisCommand(redist,op,data,partition):
    cmdData={'op':op,'part':partition,'size':data.embedding_size}
    if op=='create':
        cmdData['embedding']=data.embedding
    redist.publish(rediscommandsubs,json.dumps(cmdData))
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
    embedding_size=128
    for cat in category:
        ClusterPartition.delay(cat.name,cat.entity,embedding_size)

@celery.task
async def ClusterPartition(category,entity,embedding_size):
    db=db_engine()
    q = await db.query(models.Face).filter(models.Face.category_name == category and models.Face.status == 'ACTIVE' and models.Face.embedding_size== embedding_size and models.Face.entity==entity)
    cluster_name = entity+category
    level = 0
    maxcluster = 2
    maxbatch = 50
    minbatch = 10
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
           redis_object=[]
           redis_embedding=[]
           cluster_id=[]
           for ob in clusterlist[ind]:
              redis_object.append({"id": ob.id, "embedding": ob.embedding, "processId": ob.processIdentifier})
              ob.status = "LISTED"
              cluster_id.append(str(ob.id)+'::'+ob.processIdentifier)
              redis_embedding.extend(ob.embedding)
              ob.redisPartitionKey='id:' + cluster_name + ':' + str(ind)
           print(cluster_name + ':' + str(ind) + '=' + str(len(redis_object))+' branchend')
 #          redist.set(cluster_name + ':' + str(ind), pickle.dumps(redis_object))
           redist.rpush('embedding:'+cluster_name + ':' + str(ind),*redis_embedding)
           redist.rpush('id:'+cluster_name + ':' + str(ind),*cluster_id)
           filename=('backup/' + cluster_name + ':' + str(ind) + '.pickle').replace(':','&')
           with open(filename, 'wb') as handle:
               pickle.dump(redis_object, handle)
           print('cluster:'+'id:'+cluster_name + ':' + str(ind) +'::' + len(redis_object))
           if (len(redis_object) > maxbatch + minbatch) or len(redis_object) < minbatch:
               print("Limit Exceeded")
      else:
          redis_object=[]
          redis_embedding = []
          cluster_id = []
          for ob in clusterlist[ind]:
             redis_object.append({"id":ob.id,"embedding":ob.embedding,"processId":ob.processIdentifier})
             ob.status="LISTED"
             cluster_id.append(str(ob.id) + '::' + ob.processIdentifier)
             redis_embedding.extend(ob.embedding)
             ob.redisPartitionKey = cluster_name + ':' + str(ind)
          #redist.set(cluster_name+':'+str(ind),pickle.dumps(redis_object))
          redist.rpush('embedding:'+ cluster_name + ':' + str(ind),*redis_embedding)
          redist.rpush('id:'+cluster_name + ':' + str(ind),*cluster_id)
          filename = ('backup/' + cluster_name + ':' + str(ind) + '.pickle').replace(':', '&')
          with open(filename, 'wb') as handle:
              pickle.dump(redis_object, handle)
          med.append([])
          print('cluster:'+'id:'+cluster_name + ':' + str(ind) +'::' + len(redis_object))
      mediods.append(med)
      ind=ind+1
  return mediods

# To run locally
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)