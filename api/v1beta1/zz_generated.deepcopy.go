//go:build !ignore_autogenerated

/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by controller-gen. DO NOT EDIT.

package v1beta1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ConfigMapKeyValue) DeepCopyInto(out *ConfigMapKeyValue) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ConfigMapKeyValue.
func (in *ConfigMapKeyValue) DeepCopy() *ConfigMapKeyValue {
	if in == nil {
		return nil
	}
	out := new(ConfigMapKeyValue)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *GrpcSpec) DeepCopyInto(out *GrpcSpec) {
	*out = *in
	if in.Port != nil {
		in, out := &in.Port, &out.Port
		*out = new(int32)
		**out = **in
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(v1.ResourceRequirements)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new GrpcSpec.
func (in *GrpcSpec) DeepCopy() *GrpcSpec {
	if in == nil {
		return nil
	}
	out := new(GrpcSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ModelRegistry) DeepCopyInto(out *ModelRegistry) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ModelRegistry.
func (in *ModelRegistry) DeepCopy() *ModelRegistry {
	if in == nil {
		return nil
	}
	out := new(ModelRegistry)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ModelRegistry) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ModelRegistryList) DeepCopyInto(out *ModelRegistryList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ModelRegistry, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ModelRegistryList.
func (in *ModelRegistryList) DeepCopy() *ModelRegistryList {
	if in == nil {
		return nil
	}
	out := new(ModelRegistryList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ModelRegistryList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ModelRegistrySpec) DeepCopyInto(out *ModelRegistrySpec) {
	*out = *in
	in.Rest.DeepCopyInto(&out.Rest)
	in.Grpc.DeepCopyInto(&out.Grpc)
	if in.Postgres != nil {
		in, out := &in.Postgres, &out.Postgres
		*out = new(PostgresConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.MySQL != nil {
		in, out := &in.MySQL, &out.MySQL
		*out = new(MySQLConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.EnableDatabaseUpgrade != nil {
		in, out := &in.EnableDatabaseUpgrade, &out.EnableDatabaseUpgrade
		*out = new(bool)
		**out = **in
	}
	if in.DowngradeDbSchemaVersion != nil {
		in, out := &in.DowngradeDbSchemaVersion, &out.DowngradeDbSchemaVersion
		*out = new(int64)
		**out = **in
	}
	if in.OAuthProxy != nil {
		in, out := &in.OAuthProxy, &out.OAuthProxy
		*out = new(OAuthProxyConfig)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ModelRegistrySpec.
func (in *ModelRegistrySpec) DeepCopy() *ModelRegistrySpec {
	if in == nil {
		return nil
	}
	out := new(ModelRegistrySpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ModelRegistryStatus) DeepCopyInto(out *ModelRegistryStatus) {
	*out = *in
	if in.Hosts != nil {
		in, out := &in.Hosts, &out.Hosts
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ModelRegistryStatus.
func (in *ModelRegistryStatus) DeepCopy() *ModelRegistryStatus {
	if in == nil {
		return nil
	}
	out := new(ModelRegistryStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MySQLConfig) DeepCopyInto(out *MySQLConfig) {
	*out = *in
	if in.Port != nil {
		in, out := &in.Port, &out.Port
		*out = new(int32)
		**out = **in
	}
	if in.PasswordSecret != nil {
		in, out := &in.PasswordSecret, &out.PasswordSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLCertificateSecret != nil {
		in, out := &in.SSLCertificateSecret, &out.SSLCertificateSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLKeySecret != nil {
		in, out := &in.SSLKeySecret, &out.SSLKeySecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLRootCertificateConfigMap != nil {
		in, out := &in.SSLRootCertificateConfigMap, &out.SSLRootCertificateConfigMap
		*out = new(ConfigMapKeyValue)
		**out = **in
	}
	if in.SSLRootCertificatesConfigMapName != nil {
		in, out := &in.SSLRootCertificatesConfigMapName, &out.SSLRootCertificatesConfigMapName
		*out = new(string)
		**out = **in
	}
	if in.SSLRootCertificateSecret != nil {
		in, out := &in.SSLRootCertificateSecret, &out.SSLRootCertificateSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLRootCertificatesSecretName != nil {
		in, out := &in.SSLRootCertificatesSecretName, &out.SSLRootCertificatesSecretName
		*out = new(string)
		**out = **in
	}
	if in.SSLCipher != nil {
		in, out := &in.SSLCipher, &out.SSLCipher
		*out = new(string)
		**out = **in
	}
	if in.VerifyServerCert != nil {
		in, out := &in.VerifyServerCert, &out.VerifyServerCert
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MySQLConfig.
func (in *MySQLConfig) DeepCopy() *MySQLConfig {
	if in == nil {
		return nil
	}
	out := new(MySQLConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *OAuthProxyConfig) DeepCopyInto(out *OAuthProxyConfig) {
	*out = *in
	if in.Port != nil {
		in, out := &in.Port, &out.Port
		*out = new(int32)
		**out = **in
	}
	if in.TLSCertificateSecret != nil {
		in, out := &in.TLSCertificateSecret, &out.TLSCertificateSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.TLSKeySecret != nil {
		in, out := &in.TLSKeySecret, &out.TLSKeySecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.RoutePort != nil {
		in, out := &in.RoutePort, &out.RoutePort
		*out = new(int32)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new OAuthProxyConfig.
func (in *OAuthProxyConfig) DeepCopy() *OAuthProxyConfig {
	if in == nil {
		return nil
	}
	out := new(OAuthProxyConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *PostgresConfig) DeepCopyInto(out *PostgresConfig) {
	*out = *in
	if in.Port != nil {
		in, out := &in.Port, &out.Port
		*out = new(int32)
		**out = **in
	}
	if in.PasswordSecret != nil {
		in, out := &in.PasswordSecret, &out.PasswordSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLCertificateSecret != nil {
		in, out := &in.SSLCertificateSecret, &out.SSLCertificateSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLKeySecret != nil {
		in, out := &in.SSLKeySecret, &out.SSLKeySecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLPasswordSecret != nil {
		in, out := &in.SSLPasswordSecret, &out.SSLPasswordSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
	if in.SSLRootCertificateConfigMap != nil {
		in, out := &in.SSLRootCertificateConfigMap, &out.SSLRootCertificateConfigMap
		*out = new(ConfigMapKeyValue)
		**out = **in
	}
	if in.SSLRootCertificateSecret != nil {
		in, out := &in.SSLRootCertificateSecret, &out.SSLRootCertificateSecret
		*out = new(SecretKeyValue)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new PostgresConfig.
func (in *PostgresConfig) DeepCopy() *PostgresConfig {
	if in == nil {
		return nil
	}
	out := new(PostgresConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RestSpec) DeepCopyInto(out *RestSpec) {
	*out = *in
	if in.Port != nil {
		in, out := &in.Port, &out.Port
		*out = new(int32)
		**out = **in
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(v1.ResourceRequirements)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RestSpec.
func (in *RestSpec) DeepCopy() *RestSpec {
	if in == nil {
		return nil
	}
	out := new(RestSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SecretKeyValue) DeepCopyInto(out *SecretKeyValue) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SecretKeyValue.
func (in *SecretKeyValue) DeepCopy() *SecretKeyValue {
	if in == nil {
		return nil
	}
	out := new(SecretKeyValue)
	in.DeepCopyInto(out)
	return out
}
