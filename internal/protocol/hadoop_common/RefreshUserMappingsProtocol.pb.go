// Code generated by protoc-gen-go. DO NOT EDIT.
// source: RefreshUserMappingsProtocol.proto

package hadoop_common

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// *
//  Refresh user to group mappings request.
type RefreshUserToGroupsMappingsRequestProto struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RefreshUserToGroupsMappingsRequestProto) Reset() {
	*m = RefreshUserToGroupsMappingsRequestProto{}
}
func (m *RefreshUserToGroupsMappingsRequestProto) String() string { return proto.CompactTextString(m) }
func (*RefreshUserToGroupsMappingsRequestProto) ProtoMessage()    {}
func (*RefreshUserToGroupsMappingsRequestProto) Descriptor() ([]byte, []int) {
	return fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155, []int{0}
}
func (m *RefreshUserToGroupsMappingsRequestProto) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto.Unmarshal(m, b)
}
func (m *RefreshUserToGroupsMappingsRequestProto) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto.Marshal(b, m, deterministic)
}
func (dst *RefreshUserToGroupsMappingsRequestProto) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto.Merge(dst, src)
}
func (m *RefreshUserToGroupsMappingsRequestProto) XXX_Size() int {
	return xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto.Size(m)
}
func (m *RefreshUserToGroupsMappingsRequestProto) XXX_DiscardUnknown() {
	xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto.DiscardUnknown(m)
}

var xxx_messageInfo_RefreshUserToGroupsMappingsRequestProto proto.InternalMessageInfo

// *
// void response
type RefreshUserToGroupsMappingsResponseProto struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RefreshUserToGroupsMappingsResponseProto) Reset() {
	*m = RefreshUserToGroupsMappingsResponseProto{}
}
func (m *RefreshUserToGroupsMappingsResponseProto) String() string { return proto.CompactTextString(m) }
func (*RefreshUserToGroupsMappingsResponseProto) ProtoMessage()    {}
func (*RefreshUserToGroupsMappingsResponseProto) Descriptor() ([]byte, []int) {
	return fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155, []int{1}
}
func (m *RefreshUserToGroupsMappingsResponseProto) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto.Unmarshal(m, b)
}
func (m *RefreshUserToGroupsMappingsResponseProto) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto.Marshal(b, m, deterministic)
}
func (dst *RefreshUserToGroupsMappingsResponseProto) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto.Merge(dst, src)
}
func (m *RefreshUserToGroupsMappingsResponseProto) XXX_Size() int {
	return xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto.Size(m)
}
func (m *RefreshUserToGroupsMappingsResponseProto) XXX_DiscardUnknown() {
	xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto.DiscardUnknown(m)
}

var xxx_messageInfo_RefreshUserToGroupsMappingsResponseProto proto.InternalMessageInfo

// *
// Refresh superuser configuration request.
type RefreshSuperUserGroupsConfigurationRequestProto struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RefreshSuperUserGroupsConfigurationRequestProto) Reset() {
	*m = RefreshSuperUserGroupsConfigurationRequestProto{}
}
func (m *RefreshSuperUserGroupsConfigurationRequestProto) String() string {
	return proto.CompactTextString(m)
}
func (*RefreshSuperUserGroupsConfigurationRequestProto) ProtoMessage() {}
func (*RefreshSuperUserGroupsConfigurationRequestProto) Descriptor() ([]byte, []int) {
	return fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155, []int{2}
}
func (m *RefreshSuperUserGroupsConfigurationRequestProto) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto.Unmarshal(m, b)
}
func (m *RefreshSuperUserGroupsConfigurationRequestProto) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto.Marshal(b, m, deterministic)
}
func (dst *RefreshSuperUserGroupsConfigurationRequestProto) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto.Merge(dst, src)
}
func (m *RefreshSuperUserGroupsConfigurationRequestProto) XXX_Size() int {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto.Size(m)
}
func (m *RefreshSuperUserGroupsConfigurationRequestProto) XXX_DiscardUnknown() {
	xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto.DiscardUnknown(m)
}

var xxx_messageInfo_RefreshSuperUserGroupsConfigurationRequestProto proto.InternalMessageInfo

// *
// void response
type RefreshSuperUserGroupsConfigurationResponseProto struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RefreshSuperUserGroupsConfigurationResponseProto) Reset() {
	*m = RefreshSuperUserGroupsConfigurationResponseProto{}
}
func (m *RefreshSuperUserGroupsConfigurationResponseProto) String() string {
	return proto.CompactTextString(m)
}
func (*RefreshSuperUserGroupsConfigurationResponseProto) ProtoMessage() {}
func (*RefreshSuperUserGroupsConfigurationResponseProto) Descriptor() ([]byte, []int) {
	return fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155, []int{3}
}
func (m *RefreshSuperUserGroupsConfigurationResponseProto) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto.Unmarshal(m, b)
}
func (m *RefreshSuperUserGroupsConfigurationResponseProto) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto.Marshal(b, m, deterministic)
}
func (dst *RefreshSuperUserGroupsConfigurationResponseProto) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto.Merge(dst, src)
}
func (m *RefreshSuperUserGroupsConfigurationResponseProto) XXX_Size() int {
	return xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto.Size(m)
}
func (m *RefreshSuperUserGroupsConfigurationResponseProto) XXX_DiscardUnknown() {
	xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto.DiscardUnknown(m)
}

var xxx_messageInfo_RefreshSuperUserGroupsConfigurationResponseProto proto.InternalMessageInfo

func init() {
	proto.RegisterType((*RefreshUserToGroupsMappingsRequestProto)(nil), "hadoop.common.RefreshUserToGroupsMappingsRequestProto")
	proto.RegisterType((*RefreshUserToGroupsMappingsResponseProto)(nil), "hadoop.common.RefreshUserToGroupsMappingsResponseProto")
	proto.RegisterType((*RefreshSuperUserGroupsConfigurationRequestProto)(nil), "hadoop.common.RefreshSuperUserGroupsConfigurationRequestProto")
	proto.RegisterType((*RefreshSuperUserGroupsConfigurationResponseProto)(nil), "hadoop.common.RefreshSuperUserGroupsConfigurationResponseProto")
}

func init() {
	proto.RegisterFile("RefreshUserMappingsProtocol.proto", fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155)
}

var fileDescriptor_RefreshUserMappingsProtocol_881baac0fe66e155 = []byte{
	// 249 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x52, 0x0c, 0x4a, 0x4d, 0x2b,
	0x4a, 0x2d, 0xce, 0x08, 0x2d, 0x4e, 0x2d, 0xf2, 0x4d, 0x2c, 0x28, 0xc8, 0xcc, 0x4b, 0x2f, 0x0e,
	0x28, 0xca, 0x2f, 0xc9, 0x4f, 0xce, 0xcf, 0xd1, 0x2b, 0x00, 0x31, 0x84, 0x78, 0x33, 0x12, 0x53,
	0xf2, 0xf3, 0x0b, 0xf4, 0x92, 0xf3, 0x73, 0x73, 0xf3, 0xf3, 0x94, 0x34, 0xb9, 0xd4, 0x91, 0xf4,
	0x84, 0xe4, 0xbb, 0x17, 0xe5, 0x97, 0x16, 0x14, 0xc3, 0xf4, 0x06, 0xa5, 0x16, 0x96, 0xa6, 0x16,
	0x97, 0x80, 0x8d, 0x50, 0xd2, 0xe2, 0xd2, 0xc0, 0xab, 0xb4, 0xb8, 0x20, 0x3f, 0xaf, 0x38, 0x15,
	0xa2, 0xd6, 0x90, 0x4b, 0x1f, 0xaa, 0x36, 0xb8, 0xb4, 0x20, 0xb5, 0x08, 0xa4, 0x01, 0xa2, 0xdc,
	0x39, 0x3f, 0x2f, 0x2d, 0x33, 0xbd, 0xb4, 0x28, 0xb1, 0x24, 0x33, 0x3f, 0x0f, 0xc5, 0x78, 0x23,
	0x2e, 0x03, 0xa2, 0xb4, 0x20, 0x59, 0x63, 0x74, 0x8f, 0x89, 0x4b, 0x09, 0x8f, 0x97, 0x83, 0x53,
	0x8b, 0xca, 0x32, 0x93, 0x53, 0x85, 0xfa, 0x18, 0xb9, 0xa4, 0x8b, 0x70, 0x3b, 0x5d, 0xc8, 0x4c,
	0x0f, 0x25, 0x50, 0xf4, 0x88, 0x0c, 0x11, 0x29, 0x73, 0x52, 0xf4, 0x21, 0xb9, 0x5b, 0x68, 0x19,
	0x23, 0x97, 0x72, 0x11, 0x61, 0xcf, 0x0a, 0xd9, 0x61, 0xb7, 0x80, 0xd8, 0x30, 0x95, 0xb2, 0x27,
	0x47, 0x3f, 0x92, 0x43, 0x9d, 0xbc, 0xb9, 0x14, 0xf2, 0x8b, 0xd2, 0xf5, 0x12, 0x0b, 0x12, 0x93,
	0x33, 0x52, 0x61, 0x86, 0x15, 0xa7, 0x26, 0x97, 0x16, 0x65, 0x96, 0x54, 0x42, 0x52, 0x94, 0x13,
	0xbe, 0x44, 0x07, 0xa6, 0x8b, 0x3b, 0x18, 0x19, 0x17, 0x30, 0x32, 0x02, 0x02, 0x00, 0x00, 0xff,
	0xff, 0xfc, 0xf2, 0x05, 0xcb, 0x9e, 0x02, 0x00, 0x00,
}
