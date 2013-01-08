from pymongo import objectid

class mongoid(objectid.ObjectId):
  def decompose(self):
    o_str = str(self)
    id_time = int(o_str[0:8], 16)
    machine = int(o_str[8:14], 16)
    pid = int(o_str[14:18], 16)
    inc = int(o_str[18:], 16)
    return { "timestamp" : id_time, "machine" : machine, "pid" : pid, "inc" : inc }

  def compose(self, elements):
    objectid_r = hex(elements["timestamp"]).replace('0x', '')
    objectid_r += hex(elements["machine"]).replace('0x', '')
    objectid_r += hex(elements["pid"]).replace('0x', '').zfill(4)
    objectid_r += hex(elements["inc"]).replace('0x', '').zfill(6)
    return objectid_r

