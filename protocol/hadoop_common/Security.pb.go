// Code generated by protoc-gen-go.
// source: Security.proto
// DO NOT EDIT!

package hadoop_common

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// *
// Security token identifier
type TokenProto struct {
	Identifier       []byte  `protobuf:"bytes,1,req,name=identifier" json:"identifier,omitempty"`
	Password         []byte  `protobuf:"bytes,2,req,name=password" json:"password,omitempty"`
	Kind             *string `protobuf:"bytes,3,req,name=kind" json:"kind,omitempty"`
	Service          *string `protobuf:"bytes,4,req,name=service" json:"service,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *TokenProto) Reset()                    { *m = TokenProto{} }
func (m *TokenProto) String() string            { return proto.CompactTextString(m) }
func (*TokenProto) ProtoMessage()               {}
func (*TokenProto) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{0} }

func (m *TokenProto) GetIdentifier() []byte {
	if m != nil {
		return m.Identifier
	}
	return nil
}

func (m *TokenProto) GetPassword() []byte {
	if m != nil {
		return m.Password
	}
	return nil
}

func (m *TokenProto) GetKind() string {
	if m != nil && m.Kind != nil {
		return *m.Kind
	}
	return ""
}

func (m *TokenProto) GetService() string {
	if m != nil && m.Service != nil {
		return *m.Service
	}
	return ""
}

type GetDelegationTokenRequestProto struct {
	Renewer          *string `protobuf:"bytes,1,req,name=renewer" json:"renewer,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *GetDelegationTokenRequestProto) Reset()                    { *m = GetDelegationTokenRequestProto{} }
func (m *GetDelegationTokenRequestProto) String() string            { return proto.CompactTextString(m) }
func (*GetDelegationTokenRequestProto) ProtoMessage()               {}
func (*GetDelegationTokenRequestProto) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{1} }

func (m *GetDelegationTokenRequestProto) GetRenewer() string {
	if m != nil && m.Renewer != nil {
		return *m.Renewer
	}
	return ""
}

type GetDelegationTokenResponseProto struct {
	Token            *TokenProto `protobuf:"bytes,1,opt,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *GetDelegationTokenResponseProto) Reset()                    { *m = GetDelegationTokenResponseProto{} }
func (m *GetDelegationTokenResponseProto) String() string            { return proto.CompactTextString(m) }
func (*GetDelegationTokenResponseProto) ProtoMessage()               {}
func (*GetDelegationTokenResponseProto) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{2} }

func (m *GetDelegationTokenResponseProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type RenewDelegationTokenRequestProto struct {
	Token            *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *RenewDelegationTokenRequestProto) Reset()         { *m = RenewDelegationTokenRequestProto{} }
func (m *RenewDelegationTokenRequestProto) String() string { return proto.CompactTextString(m) }
func (*RenewDelegationTokenRequestProto) ProtoMessage()    {}
func (*RenewDelegationTokenRequestProto) Descriptor() ([]byte, []int) {
	return fileDescriptor3, []int{3}
}

func (m *RenewDelegationTokenRequestProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type RenewDelegationTokenResponseProto struct {
	NewExpiryTime    *uint64 `protobuf:"varint,1,req,name=newExpiryTime" json:"newExpiryTime,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *RenewDelegationTokenResponseProto) Reset()         { *m = RenewDelegationTokenResponseProto{} }
func (m *RenewDelegationTokenResponseProto) String() string { return proto.CompactTextString(m) }
func (*RenewDelegationTokenResponseProto) ProtoMessage()    {}
func (*RenewDelegationTokenResponseProto) Descriptor() ([]byte, []int) {
	return fileDescriptor3, []int{4}
}

func (m *RenewDelegationTokenResponseProto) GetNewExpiryTime() uint64 {
	if m != nil && m.NewExpiryTime != nil {
		return *m.NewExpiryTime
	}
	return 0
}

type CancelDelegationTokenRequestProto struct {
	Token            *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *CancelDelegationTokenRequestProto) Reset()         { *m = CancelDelegationTokenRequestProto{} }
func (m *CancelDelegationTokenRequestProto) String() string { return proto.CompactTextString(m) }
func (*CancelDelegationTokenRequestProto) ProtoMessage()    {}
func (*CancelDelegationTokenRequestProto) Descriptor() ([]byte, []int) {
	return fileDescriptor3, []int{5}
}

func (m *CancelDelegationTokenRequestProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type CancelDelegationTokenResponseProto struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *CancelDelegationTokenResponseProto) Reset()         { *m = CancelDelegationTokenResponseProto{} }
func (m *CancelDelegationTokenResponseProto) String() string { return proto.CompactTextString(m) }
func (*CancelDelegationTokenResponseProto) ProtoMessage()    {}
func (*CancelDelegationTokenResponseProto) Descriptor() ([]byte, []int) {
	return fileDescriptor3, []int{6}
}

func init() {
	proto.RegisterType((*TokenProto)(nil), "hadoop.common.TokenProto")
	proto.RegisterType((*GetDelegationTokenRequestProto)(nil), "hadoop.common.GetDelegationTokenRequestProto")
	proto.RegisterType((*GetDelegationTokenResponseProto)(nil), "hadoop.common.GetDelegationTokenResponseProto")
	proto.RegisterType((*RenewDelegationTokenRequestProto)(nil), "hadoop.common.RenewDelegationTokenRequestProto")
	proto.RegisterType((*RenewDelegationTokenResponseProto)(nil), "hadoop.common.RenewDelegationTokenResponseProto")
	proto.RegisterType((*CancelDelegationTokenRequestProto)(nil), "hadoop.common.CancelDelegationTokenRequestProto")
	proto.RegisterType((*CancelDelegationTokenResponseProto)(nil), "hadoop.common.CancelDelegationTokenResponseProto")
}

var fileDescriptor3 = []byte{
	// 287 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xac, 0x92, 0xcf, 0x4a, 0xc3, 0x40,
	0x10, 0xc6, 0xd9, 0x18, 0xd1, 0x8e, 0xad, 0x4a, 0x40, 0x88, 0x17, 0x4d, 0x17, 0x0f, 0x39, 0x05,
	0xf4, 0x24, 0x1e, 0xab, 0xe2, 0x41, 0x05, 0xad, 0x7d, 0x81, 0x25, 0x19, 0xdb, 0xa5, 0xcd, 0xce,
	0xba, 0xbb, 0xb5, 0xf6, 0x0d, 0x7c, 0x0c, 0x1f, 0xd5, 0x64, 0xeb, 0x9f, 0x14, 0x6a, 0x4f, 0x1e,
	0xf3, 0x65, 0x7f, 0xbf, 0xf9, 0x06, 0x06, 0x76, 0x9f, 0x30, 0x9f, 0x1a, 0xe9, 0xe6, 0x99, 0x36,
	0xe4, 0x28, 0xea, 0x8c, 0x44, 0x41, 0xa4, 0xb3, 0x9c, 0xca, 0x92, 0x14, 0x7f, 0x04, 0x18, 0xd0,
	0x18, 0xd5, 0x83, 0xff, 0x19, 0x01, 0xc8, 0x02, 0x95, 0x93, 0xcf, 0x12, 0x4d, 0xcc, 0x92, 0x20,
	0x6d, 0x47, 0xfb, 0xb0, 0xad, 0x85, 0xb5, 0x33, 0x32, 0x45, 0x1c, 0xf8, 0xa4, 0x0d, 0xe1, 0x58,
	0xaa, 0x22, 0xde, 0xa8, 0xbe, 0x5a, 0xd1, 0x1e, 0x6c, 0x59, 0x34, 0xaf, 0x32, 0xc7, 0x38, 0xac,
	0x03, 0x7e, 0x0a, 0x47, 0x37, 0xe8, 0xae, 0x70, 0x82, 0x43, 0xe1, 0x24, 0x29, 0xef, 0xef, 0xe3,
	0xcb, 0x14, 0xad, 0x5b, 0x8c, 0xa9, 0x10, 0x83, 0x0a, 0x67, 0x5f, 0x33, 0x5a, 0xfc, 0x16, 0x8e,
	0x57, 0x21, 0x56, 0x93, 0xb2, 0xb8, 0x60, 0x52, 0xd8, 0x74, 0x75, 0x5a, 0x11, 0x2c, 0xdd, 0x39,
	0x3b, 0xcc, 0x96, 0xf6, 0xc8, 0x7e, 0x97, 0xe0, 0x77, 0x90, 0xf4, 0x6b, 0xfb, 0xba, 0x06, 0x0d,
	0x5b, 0xb0, 0xde, 0x76, 0x01, 0xdd, 0xd5, 0xb6, 0x66, 0xb9, 0x03, 0xe8, 0x54, 0x4f, 0xae, 0xdf,
	0xb4, 0x34, 0xf3, 0x81, 0x2c, 0xd1, 0x6b, 0x43, 0x7e, 0x0f, 0xdd, 0x4b, 0xa1, 0x72, 0x9c, 0xfc,
	0x4f, 0x95, 0x13, 0xe0, 0x7f, 0xe8, 0x1a, 0x5d, 0x7a, 0xe7, 0x90, 0x90, 0x19, 0x66, 0x42, 0x8b,
	0x7c, 0x84, 0xdf, 0x32, 0xbb, 0x74, 0x04, 0xbd, 0x9f, 0xa3, 0xf0, 0x88, 0x7d, 0x67, 0xec, 0x83,
	0xb1, 0xcf, 0x00, 0x00, 0x00, 0xff, 0xff, 0xf1, 0xb9, 0x1b, 0x60, 0x2b, 0x02, 0x00, 0x00,
}
