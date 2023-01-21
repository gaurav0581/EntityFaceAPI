from datetime import datetime
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import json
import os
import jwt
from starlette.responses import JSONResponse

ACCESS_LEVEL = ['REVIEW', 'APPROVE', 'UPDATE', 'REQUEST', 'VIEW', "*"]
ROLEHIERARCHY = ["OWNER", "HR", "MANAGER", "SUPERVISOR", "TEAMLEADER", "TEAMMEMBER", "*"]
class SecureAPIMiddleware(BaseHTTPMiddleware):
  def __init__(
            self,
            app,
            some_attribute: str,
    ):
        super().__init__(app)
        file = open(os.environ['PERMISSIONFILE'])
        self.perm = json.load(file)
        self.tokenkey=os.environ['TOKEN_KEY']
        
#usage-provide the path for permission.json in environment parm
# [
# {
  # "URL": "/generateclaim",
  # "METHOD": "POST",
  # "ROLES": ["*:INVENTORY","MANAGER:*"], 
  # "AndPermission":[],
  # "OrPermission":[],
  # "SuperPermission": ["SYSCON"],
  # "includeRoles":true,
  # "IncludeModulePermissions":[]
  # }
# ]
#app = FastAPI()
#app.add_middleware(SecureAPIMiddleware, some_attribute="some_attribute_here_if_needed")

#@app.middleware("http")
  async def dispatch(self,request: Request, call_next):
    temp = [d for d in self.perm if (d['URL'] == request.url.path and d["METHOD"] == request.method)]
    access = True
    moduleaccess = {}
    deptrole = {}
    if(temp):
       try:
        token = jwt.decode(request.headers["authorization"].split("Bearer ")[1], self.tokenkey, algorithms="HS256")
        if token["exp"] < datetime.now().strftime('%y%m%d%H%M%S'): #fix
           return JSONResponse(status_code=401, content={'reason': "expired token"})

       except:
           return JSONResponse(status_code=403, content={'reason': "invalid token"})
       try:
        superaccess=False
        if "SuperPermission" in temp[0] and temp[0]["SuperPermission"]:
           superaccess=True
           for p in temp[0]["SuperPermission"]:
             if not(p in token["globalpermission"]):
                   superaccess = False
        if not superaccess:
         temp1=[d for d in token["ROLESPERMISSION"] if (d['ENTITY'] == int(request.headers["Entity"]))]
         if "Exception" in temp[0] and temp[0]["Exception"]:
             for perm in temp[0]["Exception"]:
                 if perm in temp[0]["PERMISSION"]:
                     JSONResponse(status_code=401, content={'reason': "insufficient access"})
         if(temp1):
             if temp[0]["AndPermission"] or temp[0]["OrPermission"]:
               for perm in temp1[0]["PERMISSION"]:
                 module, access = perm.split(':')
                 moduleaccess[module] =moduleaccess[module]  if module in moduleaccess.keys() and ACCESS_LEVEL.index(module) > moduleaccess[module] else ACCESS_LEVEL.index(module)
             if temp[0]["ROLES"]:
               for rol in temp1[0]["ROLES"]:
                 role, dept = rol.split(':')
                 deptrole[dept] = moduleaccess[dept] if role in deptrole.keys() and ROLEHIERARCHY.index(dept) > deptrole[dept] else ROLEHIERARCHY.index(role)
             access=False
             if temp[0]["AndPermission"]:
               andaccess=True
               for p in temp[0]["AndPermission"]:
                 module, modaccess = p.split(":")
                 if module in moduleaccess.keys():
                     if moduleaccess[module] > ACCESS_LEVEL.index(modaccess):
                         andaccess = False
                 else:
                     andaccess = False
               access=andaccess
             if not access:
              roleaccess=True
              for p in temp[0]["ROLES"]:
               rrole,rdept=p.split(":")
               if rdept in deptrole.keys():
                  if deptrole[rdept]>ROLEHIERARCHY.index(rrole):
                      roleaccess=False
               elif rdept=='*':
                  if min(deptrole.values())>ROLEHIERARCHY.index(rrole):
                      roleaccess=False
               elif rrole=='*':
                  if not rdept in deptrole.keys():
                       roleaccess=False
               else:
                   roleaccess=False
               if '*' in deptrole.keys():
                  if deptrole['*'] < ROLEHIERARCHY.index(rrole):
                       roleaccess = True

               if roleaccess:
                   access=True
                   break;
              if not roleaccess:
                  access=False
              for p in temp[0]["OrPermission"]:
                 module, modaccess = p.split(":")
                 if module in moduleaccess.keys():
                     if deptrole[module] > ACCESS_LEVEL.index(modaccess):
                         access = False
                 else:
                     access = False
         else:
             access=False
       except Exception as e:
           return JSONResponse(status_code=403, content={'reason': "invalid token"})
       if(not access):
           return JSONResponse(status_code=401, content={'reason': "permission denied"})
    else:
       return JSONResponse(status_code=404, content={'reason': "not found"})
 #   body = await request.body()
 #   jsonbody = await request.json()
    try:
      roleheader=''
      for x in deptrole.keys():
          roleheader=roleheader+ x+':'+ROLEHIERARCHY[deptrole[x]]+','
      permheader=''
      for mod in temp[0]["IncludeModulePermissions"]:
          if mod in moduleaccess:
             permheader=permheader+ x+':'+ACCESS_LEVEL[moduleaccess[mod]]+','
      request.headers.__dict__["_list"].append(
        (
            "X-Entity",
            temp1[0]["ENTITY"]
        ))
      if permheader:
        request.headers.__dict__["_list"].append((
                "X-Permission",
                permheader
            ))
      request.headers.__dict__["_list"].append((
                "X-User",
                token["ID"]
            ))
      if roleheader:
        request.headers.__dict__["_list"].append((
                "X-Role",
                roleheader
            ))
    except Exception as e:
         return JSONResponse(status_code=403, content={'reason': "invalid token"})
    response = await call_next(request)
    return response

def get_header(request):
    role=[]
    permission=[]
    for header in request.headers.raw:
        if header[0]=="X-User":
            user=header[1]
        elif header[0]=="X-Role":
            role=[rol.split(':') for rol in header[1].split(',') if rol]
        elif header[0] == "X-Entity":
            entity = header[1]
        elif header[0] == "X-Permission":
            permission=[rol.split(':') for rol in header[1].split(',') if rol]
    return {'user':user,'entity':entity,'role':role,'permission':permission}
