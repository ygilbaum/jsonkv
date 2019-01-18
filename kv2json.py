#!/usr/bin/python3

import consul
import os
import socket
import sys
import argparse
import random
import json

workey = f'YGWK{random.randint(100000,3000000)}'

"""
printhelp() - prints help message and exits
"""
def printhelp():
  print (f'{os.path.basename(sys.argv[0])} <keyname> [-s|--server <consul server addr (def. localhost)>] [-p|--port <consul server port (def. 8500)>] [-f|--format]')
  sys.exit(0)

"""
getKey(cons, key) - recursevly loads data from consul KV db. 
Returns dictionary with the data: {workey: ((key,val),....)}
"""
def getKey(cons, key):
  keylen = len(key)
  rawlist = []
  (ind,keys) = cons.kv.get(key, recurse=True, keys=True)
  for key in keys:
    (ind,val) = cons.kv.get(key)
    bstr = val["Value"]
    if type(bstr) == bytes:
      try:
        rawlist.append((val["Key"][keylen + 1:],bstr.decode()))
      except:
        if binary:
          rawlist.append((val["Key"][keylen + 1:],bstr))
    else:
      if not val["Key"].endswith('/'):
        rawlist.append((val["Key"][keylen + 1:],''))
  return {workey:tuple(rawlist)}

"""
getFirstKey(stri) - retrieves and returns first key in the key path. For 'a/b/c/d' returns 'a'
"""
def getFirstKey(stri):
  return stri.split('/')[0]

"""
getSecondKey(stri) - retrieves and returns second key in the key path. For 'a/b/c/d' returns 'b'
"""
def getSecondKey(stri):
  return stri.split('/')[1]
"""
getNextKey(stri) - retrieves and returns key after the first key in the path. For 'a/b/c/d' returns 'b/c/d'
"""
def getNextKey(stri):
  return stri[len(getFirstKey(stri)) + 1:]

"""
isList(keystr): checks if the key in key - value pair is integer string. If yes, 
assumes that it's an element of array and returns True.
"""
def isList(keystr):
  try:
    if str(int(keystr)) == keystr:
      return True
  except:
    None
  return False

"""
runRaw(workdict) - converts tulple of tuples in {workey: ((key,val),....)} dict to nested dictionaries
"""
def runRaw(workdict):
  tpl = workdict[workey]
  resdict = {} # result dictionary

  while len(tpl) > 0:
    first = getFirstKey(tpl[0][0])
    first_len = len(first)
    work = [] # next level list
    dic = {} # current level dict
    newraw = [] # list of keys we don't process now
    for member in tpl:
      key = member[0]
      if getFirstKey(key) == first:
        val = member[1]
        key = getNextKey(key)
        if key != '':
          work.append((key,val))
        else:
          resdict[first] = val
      else:
        newraw.append(member)

    if len(work) > 0: resdict[first] = runRaw({workey:tuple(work)})
    tpl = tuple(newraw)
  return resdict


"""
setLists(rawdict) - Takes the dictionary, created by runRaw function and if keys in one of the nested dictionaries are integer numbers, convert the dictionary to a list
"""
def setLists(rawdict):
  newdict = {}

  for key,val in rawdict.items():
    if type(val) == dict:
      mykey = key
      myval = val
      if isList(list(myval.keys())[0]):
        try:
          if type(newdict[mykey]) != list:
            newdict[mykey] = []
        except:
            newdict[mykey] = []

        for key,val in myval.items():
          if type(val) == dict:
            newdict[mykey].append(setLists(val))
          else:
            newdict[mykey].append(val)
      else:
        newdict[mykey] = setLists(myval)
    else:
      newdict[key] = val

  return newdict


if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Gets key', add_help=False)

  parser.add_argument('key', nargs='?')

  parser.add_argument('-h', '--help', default = False, action='store_true')
  parser.add_argument('-f', '--format', default = False, action='store_true')
  parser.add_argument('-p', '--port', default = '8500')
  parser.add_argument('-s', '--server', default = '127.0.0.1')

  args = parser.parse_args()

  if args.help:
    printhelp()

  server = args.server
  port = args.port

  if args.key:
    key_name = args.key
  else:
    print ('key name to get is not provided')
    printhelp()

  cons = consul.Consul(host=server,port=port)

  rawdict = getKey(cons, key_name)
  rawdict = runRaw(rawdict)
  rawdict = setLists(rawdict)

  rawstr = f'{rawdict}'.replace('\'','"')

  if args.format:
    print (json.dumps(json.loads(rawstr), sort_keys=True, indent=4))
  else:
    print (f'{rawstr}')


