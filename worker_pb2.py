# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: worker.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cworker.proto\x12\rworkerPackage\"\x07\n\x05\x65mpty\"\x1a\n\x0ckmeansReduce\x12\n\n\x02id\x18\x01 \x01(\x05\"#\n\x06status\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x05\x12\x0b\n\x03msg\x18\x02 \x01(\t\"R\n\x0bkmeansInput\x12\x0c\n\x04path\x18\x01 \x01(\t\x12\r\n\x05mapID\x18\x02 \x01(\x05\x12\x13\n\x0bnumClusters\x18\x03 \x01(\x05\x12\x11\n\tcentroids\x18\x04 \x03(\x05\"\x1a\n\ndriverPort\x12\x0c\n\x04port\x18\x01 \x01(\x05\x32\xf7\x01\n\x06Worker\x12\x41\n\rsetDriverPort\x12\x19.workerPackage.driverPort\x1a\x15.workerPackage.status\x12\x38\n\x03map\x12\x1a.workerPackage.kmeansInput\x1a\x15.workerPackage.status\x12<\n\x06reduce\x12\x1b.workerPackage.kmeansReduce\x1a\x15.workerPackage.status\x12\x32\n\x03\x64ie\x12\x14.workerPackage.empty\x1a\x15.workerPackage.statusb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'worker_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_EMPTY']._serialized_start=31
  _globals['_EMPTY']._serialized_end=38
  _globals['_KMEANSREDUCE']._serialized_start=40
  _globals['_KMEANSREDUCE']._serialized_end=66
  _globals['_STATUS']._serialized_start=68
  _globals['_STATUS']._serialized_end=103
  _globals['_KMEANSINPUT']._serialized_start=105
  _globals['_KMEANSINPUT']._serialized_end=187
  _globals['_DRIVERPORT']._serialized_start=189
  _globals['_DRIVERPORT']._serialized_end=215
  _globals['_WORKER']._serialized_start=218
  _globals['_WORKER']._serialized_end=465
# @@protoc_insertion_point(module_scope)
