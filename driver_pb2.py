# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: driver.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x64river.proto\x12\rdriverPackage\"\x07\n\x05\x65mpty\"#\n\x06status\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x05\x12\x0b\n\x03msg\x18\x02 \x01(\t\"_\n\nlaunchData\x12\x0f\n\x07\x64irPath\x18\x01 \x01(\t\x12\t\n\x01m\x18\x02 \x01(\x05\x12\r\n\x05ports\x18\x03 \x01(\t\x12\x13\n\x0bnumClusters\x18\x04 \x01(\x05\x12\x11\n\tdimension\x18\x05 \x01(\x05\x32J\n\x06\x44river\x12@\n\x0claunchDriver\x12\x19.driverPackage.launchData\x1a\x15.driverPackage.statusb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'driver_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_EMPTY']._serialized_start=31
  _globals['_EMPTY']._serialized_end=38
  _globals['_STATUS']._serialized_start=40
  _globals['_STATUS']._serialized_end=75
  _globals['_LAUNCHDATA']._serialized_start=77
  _globals['_LAUNCHDATA']._serialized_end=172
  _globals['_DRIVER']._serialized_start=174
  _globals['_DRIVER']._serialized_end=248
# @@protoc_insertion_point(module_scope)
